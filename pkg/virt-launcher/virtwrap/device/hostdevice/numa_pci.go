package hostdevice

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/util/hardware"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
)

const (
	rootBusDomain    = "0x0000"
	defaultPXBSlot   = 0x0a
	maxRootBusSlot   = 0x1f
	maxRootPortSlot  = 0x1f
	unifiedNUMAGroup = 0

	defaultPXBHole64GiB          = 64
	fallbackPrefetchPerDeviceGiB = 32
	pxbPrefetchHeadroomGiB       = 16
	maxPXBHole64GiB              = 512
)

const gibibyte = uint64(1 << 30)

var (
	formatPCIAddressFunc        = hardware.FormatPCIAddress
	getDeviceNumaNodeIntFunc    = hardware.GetDeviceNumaNodeInt
	getMdevParentPCIAddressFunc = hardware.GetMdevParentPCIAddress
	getDevicePrefetchable64Func = hardware.GetDevicePrefetchable64Size
)

type numaPCIPlanner struct {
	domain              *api.Domain
	nextControllerIndex int
	nextChassis         int
	nextPXBSlot         int
	usedRootSlots       map[int]struct{}
	usedNUMAChassis     map[int]struct{}
	pxbs                map[int]*pxbInfo
}

type pxbInfo struct {
	index         int
	nextPortSlot  int
	nextPortValue int
	reusablePorts []*rootPortInfo
}

type rootPortInfo struct {
	controllerIndex int
}

func ApplyNUMAHostDeviceTopology(vmi *v1.VirtualMachineInstance, domain *api.Domain) {
	if vmi == nil || domain == nil {
		return
	}
	log.Log.V(1).Info("evaluating host device NUMA topology passthrough")
	// Host devcies NUMA passthrough only works when CPU NUMA passthrough is enabled
	// because without CPU NUMA passthrough, there will be only one NUMA node in the guest
	// so all host devices will be assigned to the same NUMA node.
	if vmi.Spec.Domain.CPU == nil ||
		vmi.Spec.Domain.CPU.NUMA == nil ||
		vmi.Spec.Domain.CPU.NUMA.GuestMappingPassthrough == nil {
		log.Log.V(1).Info("NUMA host device topology not applied: guestMappingPassthrough disabled")
		return
	}

	hostDevices := domain.Spec.Devices.HostDevices
	if len(hostDevices) == 0 {
		log.Log.V(1).Info("NUMA host device topology not applied: no host devices present")
		return
	}

	log.Log.V(1).Infof("NUMA host device topology: processing %d host devices", len(hostDevices))
	collapseNUMA := shouldCollapseHostDeviceNUMA(vmi, domain)
	if collapseNUMA {
		log.Log.V(1).Info("collapsing host device NUMA groups to a single guest NUMA node")
	}

	planner := newNUMAPCIPlanner(domain)
	grouped := make(map[int][]*api.HostDevice)
	numaPrefetch := make(map[int]uint64)
	numaPrefetchObserved := make(map[int]int)
	var totalPXBHoleGiB uint

	for i := range hostDevices {
		dev := &domain.Spec.Devices.HostDevices[i]
		if dev.Type != api.HostDevicePCI && dev.Type != api.HostDeviceMDev {
			log.Log.V(1).Infof("skipping host device %d with unsupported type %s", i, dev.Type)
			continue
		}
		bdf, err := resolveHostDevicePCIAddress(dev)
		if err != nil {
			log.Log.V(1).Reason(err).Info("unable to resolve host device PCI address for NUMA planning, skipping")
			continue
		}
		numaNode, err := getDeviceNumaNodeIntFunc(bdf)
		if err != nil || numaNode < 0 {
			if err != nil {
				log.Log.V(1).Reason(err).Infof("skipping host device %s - failed to detect NUMA node", bdf)
			} else {
				log.Log.V(1).Infof("skipping host device %s - NUMA node not available", bdf)
			}
			continue
		}
		groupKey := numaNode
		if collapseNUMA {
			log.Log.V(1).Infof("host device %s (host NUMA %d) collapsed to guest NUMA group %d", bdf, numaNode, unifiedNUMAGroup)
			groupKey = unifiedNUMAGroup
		} else {
			log.Log.V(1).Infof("host device %s grouped to NUMA node %d", bdf, groupKey)
		}
		grouped[groupKey] = append(grouped[groupKey], dev)
		prefetchSize, err := getDevicePrefetchable64Func(bdf)
		if err != nil {
			log.Log.V(1).Reason(err).Infof("unable to detect 64-bit prefetchable BAR size for host device %s, using fallback sizing", bdf)
		} else {
			numaPrefetch[groupKey] += prefetchSize
			numaPrefetchObserved[groupKey]++
		}
	}

	if len(grouped) == 0 {
		log.Log.V(1).Info("NUMA host device topology not applied: no devices with NUMA affinity detected")
		return
	}

	numaNodes := make([]int, 0, len(grouped))
	for numaNode := range grouped {
		numaNodes = append(numaNodes, numaNode)
	}
	slices.Sort(numaNodes)

	for _, numaNode := range numaNodes {
		devices := grouped[numaNode]
		if len(devices) == 0 {
			continue
		}
		log.Log.V(1).Infof("setting up PCI expander bus for NUMA node %d with %d devices", numaNode, len(devices))
		pxb, hole64GiB, err := planner.ensurePXBForNUMA(numaNode, len(devices), numaPrefetch[numaNode], numaPrefetchObserved[numaNode])
		if err != nil {
			log.Log.Reason(err).Errorf("failed to create PCI expander bus for NUMA node %d", numaNode)
			continue
		}
		totalPXBHoleGiB += hole64GiB
		for _, dev := range devices {
			rootPort, err := planner.addRootPort(pxb)
			if err != nil {
				log.Log.Reason(err).Error("failed to allocate root port for host device")
				continue
			}
			// The planner hands out each PCIe root port controller a monotonically increasing index (starting from whatever the domain already used).
			//  When we assign a host device to that root port we derive the downstream bus straight from that index:
			assignHostDeviceToRootPort(dev, rootPort)
			log.Log.V(1).Infof("assigned host device to NUMA node %d bus %#02x", numaNode, rootPort.controllerIndex)
		}
	}

	if totalPXBHoleGiB > 0 {
		ensureRootPCIHole64(planner.domain, totalPXBHoleGiB)
	}
}

func newNUMAPCIPlanner(domain *api.Domain) *numaPCIPlanner {
	planner := &numaPCIPlanner{
		domain:          domain,
		nextPXBSlot:     defaultPXBSlot,
		usedRootSlots:   map[int]struct{}{},
		usedNUMAChassis: map[int]struct{}{},
		pxbs:            map[int]*pxbInfo{},
		nextChassis:     1,
	}

	maxIndex := -1
	pxbByIndex := map[int]*pxbInfo{}

	for i := range domain.Spec.Devices.Controllers {
		ctrl := domain.Spec.Devices.Controllers[i]
		if idx, err := strconv.Atoi(ctrl.Index); err == nil && idx > maxIndex {
			maxIndex = idx
		}
		if ctrl.Address != nil &&
			strings.EqualFold(strings.TrimPrefix(ctrl.Address.Domain, "0x"), "0000") &&
			strings.EqualFold(strings.TrimPrefix(ctrl.Address.Bus, "0x"), "00") &&
			ctrl.Address.Slot != "" {
			if slotVal, err := strconv.ParseInt(strings.TrimPrefix(ctrl.Address.Slot, "0x"), 16, 32); err == nil {
				planner.usedRootSlots[int(slotVal)] = struct{}{}
				if int(slotVal) >= planner.nextPXBSlot {
					planner.nextPXBSlot = int(slotVal) + 1
				}
			}
		}
		// Track existing chassis numbers to avoid conflicts
		if ctrl.Target != nil && ctrl.Target.Chassis != "" {
			if chassisVal, err := strconv.Atoi(ctrl.Target.Chassis); err == nil {
				planner.usedNUMAChassis[chassisVal] = struct{}{}
				if chassisVal >= planner.nextChassis {
					planner.nextChassis = chassisVal + 1
				}
			}
		}

		if ctrl.Model == "pcie-expander-bus" {
			if ctrl.Target == nil || ctrl.Target.Node == nil {
				continue
			}
			nodeID := *ctrl.Target.Node
			indexVal, err := strconv.Atoi(ctrl.Index)
			if err != nil {
				continue
			}

			info := &pxbInfo{
				index: indexVal,
			}
			planner.pxbs[nodeID] = info
			pxbByIndex[indexVal] = info

			// Ensure slot bookkeeping honours already allocated root bus slots
			if ctrl.Address != nil && ctrl.Address.Slot != "" {
				if slotVal, err := strconv.ParseInt(strings.TrimPrefix(ctrl.Address.Slot, "0x"), 16, 32); err == nil {
					if int(slotVal) >= planner.nextPXBSlot {
						planner.nextPXBSlot = int(slotVal) + 1
					}
				}
			}
			continue
		}

		if ctrl.Model == "pcie-root-port" && ctrl.Address != nil {
			busVal, err := strconv.ParseInt(strings.TrimPrefix(ctrl.Address.Bus, "0x"), 16, 32)
			if err != nil {
				continue
			}
			info, ok := pxbByIndex[int(busVal)]
			if !ok {
				continue
			}

			controllerIndex, err := strconv.Atoi(ctrl.Index)
			if err != nil {
				continue
			}
			info.reusablePorts = append(info.reusablePorts, &rootPortInfo{
				controllerIndex: controllerIndex,
			})

			if ctrl.Address.Slot != "" {
				if slotVal, err := strconv.ParseInt(strings.TrimPrefix(ctrl.Address.Slot, "0x"), 16, 32); err == nil {
					if slot := int(slotVal) + 1; slot > info.nextPortSlot {
						info.nextPortSlot = slot
					}
				}
			}

			if ctrl.Target != nil && ctrl.Target.Port != "" {
				if portVal, err := strconv.ParseInt(strings.TrimPrefix(ctrl.Target.Port, "0x"), 16, 32); err == nil {
					if port := int(portVal) + 1; port > info.nextPortValue {
						info.nextPortValue = port
					}
				}
			}

			continue
		}
	}

	for _, info := range planner.pxbs {
		if info.nextPortSlot == 0 {
			info.nextPortSlot = 0
		}
		if info.nextPortValue == 0 {
			info.nextPortValue = 0
		}
		if len(info.reusablePorts) > 0 {
			slices.SortFunc(info.reusablePorts, func(a, b *rootPortInfo) int {
				return a.controllerIndex - b.controllerIndex
			})
		}
	}

	planner.nextControllerIndex = maxIndex + 1
	if planner.nextPXBSlot > maxRootBusSlot {
		planner.nextPXBSlot = maxRootBusSlot
	}
	return planner
}

func (p *numaPCIPlanner) ensurePXBForNUMA(numa int, deviceCount int, measuredPrefetch uint64, measuredDevices int) (*pxbInfo, uint, error) {
	if existing, ok := p.pxbs[numa]; ok {
		reservationGiB := calculatePXBHole64GiB(deviceCount, measuredDevices, measuredPrefetch)
		return existing, reservationGiB, nil
	}
	index := p.nextControllerIndex
	p.nextControllerIndex++

	slot := p.allocateRootSlot()
	if slot < 0 {
		return nil, 0, fmt.Errorf("no slots available for PCI expander bus")
	}

	nodeVal := numa
	controller := api.Controller{
		Type:  "pci",
		Index: strconv.Itoa(index),
		Model: "pcie-expander-bus",
		ModelInfo: &api.ControllerModel{
			Name: "pxb-pcie",
		},
		Target: &api.ControllerTarget{
			Node: &nodeVal,
		},
		Alias: api.NewUserDefinedAlias(fmt.Sprintf("pcie.%d", index)),
		Address: &api.Address{
			Type:     api.AddressPCI,
			Domain:   rootBusDomain,
			Bus:      "0x00",
			Slot:     fmt.Sprintf("0x%02x", slot),
			Function: "0x0",
		},
	}

	reservationGiB := calculatePXBHole64GiB(deviceCount, measuredDevices, measuredPrefetch)
	log.Log.V(1).Infof("NUMA node %d expander bus reserving %d GiB of 64-bit prefetchable MMIO space", numa, reservationGiB)

	p.domain.Spec.Devices.Controllers = append(p.domain.Spec.Devices.Controllers, controller)

	info := &pxbInfo{
		index:         index,
		nextPortSlot:  0,
		nextPortValue: 0,
	}
	p.pxbs[numa] = info
	return info, reservationGiB, nil
}

func (p *numaPCIPlanner) addRootPort(info *pxbInfo) (*rootPortInfo, error) {
	if len(info.reusablePorts) > 0 {
		port := info.reusablePorts[0]
		info.reusablePorts = info.reusablePorts[1:]
		return port, nil
	}

	index := p.nextControllerIndex
	p.nextControllerIndex++

	slot := info.nextPortSlot
	if slot > maxRootPortSlot {
		return nil, fmt.Errorf("no more slots available on NUMA expander bus")
	}
	info.nextPortSlot++

	// Find next available chassis number
	chassis := p.allocateChassis()
	if chassis < 0 {
		return nil, fmt.Errorf("no more chassis numbers available")
	}
	portHex := fmt.Sprintf("0x%x", info.nextPortValue)
	info.nextPortValue++

	controller := api.Controller{
		Type:  "pci",
		Index: strconv.Itoa(index),
		Model: "pcie-root-port",
		Target: &api.ControllerTarget{
			Chassis: strconv.Itoa(chassis),
			Port:    portHex,
		},
		Alias: api.NewUserDefinedAlias(fmt.Sprintf("pcie.%d", index)),
		Address: &api.Address{
			Type:     api.AddressPCI,
			Domain:   rootBusDomain,
			Bus:      fmt.Sprintf("0x%02x", info.index),
			Slot:     fmt.Sprintf("0x%02x", slot),
			Function: "0x0",
		},
	}

	p.domain.Spec.Devices.Controllers = append(p.domain.Spec.Devices.Controllers, controller)

	return &rootPortInfo{
		controllerIndex: index,
	}, nil
}

func (p *numaPCIPlanner) allocateRootSlot() int {
	for slot := p.nextPXBSlot; slot <= maxRootBusSlot; slot++ {
		if _, used := p.usedRootSlots[slot]; !used {
			p.usedRootSlots[slot] = struct{}{}
			p.nextPXBSlot = slot + 1
			return slot
		}
	}
	return -1
}

func (p *numaPCIPlanner) allocateChassis() int {
	for chassis := p.nextChassis; chassis <= 255; chassis++ {
		if _, used := p.usedNUMAChassis[chassis]; !used {
			p.usedNUMAChassis[chassis] = struct{}{}
			p.nextChassis = chassis + 1
			return chassis
		}
	}
	return -1
}

// TODO: (fanzhangio) The downstream bus assigned to each controller is derived from the controller index;
// controller indices are monotonic in the planner and a practical deployment never approaches the
// 0xff PCI bus ceiling, but if we ever expose hundreds of root ports this is the place to revisit the
// numbering scheme before libvirt complains.
func assignHostDeviceToRootPort(dev *api.HostDevice, port *rootPortInfo) {
	if dev.Address == nil {
		dev.Address = &api.Address{}
	}
	// Set NUMA-specific bus, let PCI placement handle slot assignment
	dev.Address = &api.Address{
		Type:       api.AddressPCI,
		Domain:     rootBusDomain,
		Controller: strconv.Itoa(port.controllerIndex),
		Bus:        fmt.Sprintf("0x%02x", port.controllerIndex),
		// Slot and Function left empty to trigger PCI placement logic
		Slot:     "",
		Function: "",
	}
}

func resolveHostDevicePCIAddress(dev *api.HostDevice) (string, error) {
	if dev == nil || dev.Source.Address == nil {
		return "", fmt.Errorf("host device address missing")
	}

	addr := dev.Source.Address
	if addr.UUID != "" {
		return getMdevParentPCIAddressFunc(addr.UUID)
	}
	if addr.Domain != "" && addr.Bus != "" && addr.Slot != "" && addr.Function != "" {
		return formatPCIAddressFunc(addr)
	}
	return "", fmt.Errorf("unsupported host device address format")
}

// shouldCollapseHostDeviceNUMA returns true when PCI host devices should all be mapped to a single
// guest NUMA node. This happens when CPU NUMA passthrough is enabled (already validated by the caller)
// but the converter produced only one guest NUMA cell (for example cpumanager pinned every vCPU to the
// same host NUMA cell). We rely on the converter to populate domain.Spec.CPU.NUMA after vCPU pinning;
// if this helper runs earlier in the pipeline we intentionally fall back to per-host NUMA grouping to
// preserve behaviour.
func shouldCollapseHostDeviceNUMA(vmi *v1.VirtualMachineInstance, domain *api.Domain) bool {
	if vmi == nil || domain == nil {
		return false
	}

	if domain.Spec.CPU.NUMA == nil {
		return false
	}

	return len(domain.Spec.CPU.NUMA.Cells) <= 1
}

func calculatePXBHole64GiB(deviceCount, measuredDevices int, measuredPrefetch uint64) uint {
	if deviceCount <= 0 {
		return uint(defaultPXBHole64GiB)
	}

	requiredBytes := measuredPrefetch
	if missing := deviceCount - measuredDevices; missing > 0 {
		requiredBytes += uint64(missing) * uint64(fallbackPrefetchPerDeviceGiB) * gibibyte
	}

	requiredGiB := defaultPXBHole64GiB
	if requiredBytes > 0 {
		computed := bytesToGiBCeil(requiredBytes)
		computed += pxbPrefetchHeadroomGiB
		if computed > requiredGiB {
			requiredGiB = computed
		}
	}

	if requiredGiB > maxPXBHole64GiB {
		requiredGiB = maxPXBHole64GiB
	}

	return uint(requiredGiB)
}

func bytesToGiBCeil(value uint64) int {
	if value == 0 {
		return 0
	}
	return int((value + gibibyte - 1) / gibibyte)
}

func ensureRootPCIHole64(domain *api.Domain, requiredGiB uint) {
	if domain == nil || requiredGiB == 0 {
		return
	}

	var root *api.Controller
	for i := range domain.Spec.Devices.Controllers {
		ctrl := &domain.Spec.Devices.Controllers[i]
		if ctrl.Type == "pci" && ctrl.Model == "pcie-root" {
			root = ctrl
			break
		}
	}
	if root == nil {
		// libvirt will implicitly create the root controller if it is absent.
		// However, when we need to reserve additional 64-bit MMIO space we must materialize it up-front.
		rootController := api.Controller{
			Type:  "pci",
			Index: "0",
			Model: "pcie-root",
			Alias: api.NewUserDefinedAlias("pcie.0"),
		}
		domain.Spec.Devices.Controllers = append([]api.Controller{rootController}, domain.Spec.Devices.Controllers...)
		root = &domain.Spec.Devices.Controllers[0]
		log.Log.V(1).Info("pcie-root controller materialized to expand 64-bit MMIO aperture")
	} else if root.Alias == nil {
		// Normalise alias to keep parity with libvirt auto-generated domains.
		root.Alias = api.NewUserDefinedAlias("pcie.0")
	}

	existingGiB := pciHole64ToGiB(root.PCIHole64)
	if existingGiB >= requiredGiB {
		return
	}

	requiredKiB := requiredGiB * 1024 * 1024
	root.PCIHole64 = &api.PCIHole64{
		Value: requiredKiB,
		Unit:  "KiB",
	}
	log.Log.V(1).Infof("root complex 64-bit MMIO aperture expanded to %d GiB for NUMA host devices", requiredGiB)
}

func pciHole64ToGiB(value *api.PCIHole64) uint {
	if value == nil {
		return 0
	}

	unit := strings.ToLower(value.Unit)
	switch unit {
	case "":
		return uint(bytesToGiBCeil(uint64(value.Value)))
	case "bytes":
		return uint(bytesToGiBCeil(uint64(value.Value)))
	case "kib":
		return uint(bytesToGiBCeil(uint64(value.Value) * 1024))
	case "kibibytes":
		return uint(bytesToGiBCeil(uint64(value.Value) * 1024))
	case "mib", "mibibytes":
		return uint(bytesToGiBCeil(uint64(value.Value) * 1024 * 1024))
	case "gib", "gibibytes":
		return value.Value
	case "kb":
		return uint(bytesToGiBCeil(uint64(value.Value) * 1000))
	case "mb":
		return uint(bytesToGiBCeil(uint64(value.Value) * 1000 * 1000))
	case "gb":
		return uint(bytesToGiBCeil(uint64(value.Value) * 1000 * 1000 * 1000))
	default:
		return uint(bytesToGiBCeil(uint64(value.Value)))
	}
}
