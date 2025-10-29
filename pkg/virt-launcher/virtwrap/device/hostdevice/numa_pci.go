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
	rootBusDomain           = "0x0000"
	defaultPXBSlot          = 0x0a
	maxRootBusSlot          = 0x1f
	maxRootPortSlot         = 0x1f
	unifiedNUMAGroup        = 0
	unifiedTopologyGroupKey = "collapsed"
)

var (
	formatPCIAddressFunc           = hardware.FormatPCIAddress
	getDeviceNumaNodeIntFunc       = hardware.GetDeviceNumaNodeInt
	getMdevParentPCIAddressFunc    = hardware.GetMdevParentPCIAddress
	getDevicePCIProximityGroupFunc = hardware.GetDevicePCITopologyGroup
)

type numaPCIPlanner struct {
	domain              *api.Domain
	nextControllerIndex int
	nextChassis         int
	nextPXBSlot         int
	usedRootSlots       map[int]struct{}
	usedNUMAChassis     map[int]struct{}
	pxbs                map[deviceGroupKey]*pxbInfo
}

type pxbInfo struct {
	index         int
	nextPortSlot  int
	nextPortValue int
}

type rootPortInfo struct {
	controllerIndex int
}

type deviceNUMAInfo struct {
	dev           *api.HostDevice
	hostNUMANode  int
	guestNUMANode int
	bdf           string
	topologyGroup string
}

type deviceGroupKey struct {
	guestNUMANode int
	hostNUMANode  int
	topology      string
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

	planner := newNUMAPCIPlanner(domain)
	devicesWithNUMA := make([]deviceNUMAInfo, 0, len(hostDevices))
	hostNUMANodes := make(map[int]struct{})
	guestNUMANodes := getGuestNUMANodes(domain)

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
		groupID, err := getDevicePCIProximityGroupFunc(bdf)
		if err != nil {
			log.Log.V(1).Reason(err).Infof("using default topology grouping for host device %s (host NUMA %d)", bdf, numaNode)
			groupID = fmt.Sprintf("numa-%d", numaNode)
		}
		guestNode, ok := mapHostToGuestNUMANode(numaNode, guestNUMANodes)
		if !ok {
			log.Log.V(1).Infof("host device %s (host NUMA %d) remapped to guest NUMA node %d - guest NUMA cell missing", bdf, numaNode, guestNode)
		} else {
			log.Log.V(1).Infof("host device %s aligned with guest NUMA node %d", bdf, guestNode)
		}
		devicesWithNUMA = append(devicesWithNUMA, deviceNUMAInfo{
			dev:           dev,
			hostNUMANode:  numaNode,
			guestNUMANode: guestNode,
			bdf:           bdf,
			topologyGroup: groupID,
		})
		hostNUMANodes[numaNode] = struct{}{}
	}

	if len(devicesWithNUMA) == 0 {
		log.Log.V(1).Info("NUMA host device topology not applied: no devices with NUMA affinity detected")
		return
	}

	collapseNUMA := shouldCollapseHostDeviceNUMA(vmi, domain, hostNUMANodes)
	if collapseNUMA {
		log.Log.V(1).Info("collapsing host device NUMA groups to a single guest NUMA node")
	}

	grouped := make(map[deviceGroupKey][]*api.HostDevice)
	for _, info := range devicesWithNUMA {
		groupKey := deviceGroupKey{
			hostNUMANode:  info.hostNUMANode,
			guestNUMANode: info.guestNUMANode,
			topology:      info.topologyGroup,
		}
		if collapseNUMA {
			log.Log.V(1).Infof("host device %s (host NUMA %d, topology %q) collapsed to guest NUMA group %d", info.bdf, info.hostNUMANode, info.topologyGroup, unifiedNUMAGroup)
			groupKey = deviceGroupKey{
				hostNUMANode:  unifiedNUMAGroup,
				guestNUMANode: unifiedNUMAGroup,
				topology:      unifiedTopologyGroupKey,
			}
		} else {
			log.Log.V(1).Infof("host device %s grouped to host NUMA %d (guest NUMA %d) topology %q", info.bdf, groupKey.hostNUMANode, groupKey.guestNUMANode, groupKey.topology)
		}
		grouped[groupKey] = append(grouped[groupKey], info.dev)
	}

	groupKeys := make([]deviceGroupKey, 0, len(grouped))
	for key := range grouped {
		groupKeys = append(groupKeys, key)
	}
	slices.SortFunc(groupKeys, func(a, b deviceGroupKey) int {
		if a.guestNUMANode != b.guestNUMANode {
			if a.guestNUMANode < b.guestNUMANode {
				return -1
			}
			return 1
		}
		if a.hostNUMANode != b.hostNUMANode {
			if a.hostNUMANode < b.hostNUMANode {
				return -1
			}
			return 1
		}
		if a.topology == b.topology {
			return 0
		}
		if a.topology < b.topology {
			return -1
		}
		return 1
	})

	for _, key := range groupKeys {
		devices := grouped[key]
		if len(devices) == 0 {
			continue
		}
		log.Log.V(1).Infof("setting up PCI expander bus for host NUMA %d (guest NUMA %d) topology %q with %d devices",
			key.hostNUMANode, key.guestNUMANode, key.topology, len(devices))
		pxb, err := planner.ensurePXBForGroup(key.hostNUMANode, key.guestNUMANode, key.topology)
		if err != nil {
			log.Log.Reason(err).Errorf("failed to create PCI expander bus for host NUMA %d (guest NUMA %d) topology %q",
				key.hostNUMANode, key.guestNUMANode, key.topology)
			continue
		}
		for _, dev := range devices {
			rootPort, err := planner.addRootPort(pxb)
			if err != nil {
				log.Log.Reason(err).Error("failed to allocate root port for host device")
				continue
			}
			// The planner hands out each PCIe root port controller a monotonically increasing index (starting from whatever the domain already used).
			//  When we assign a host device to that root port we derive the downstream bus straight from that index:
			assignHostDeviceToRootPort(dev, rootPort)
			log.Log.V(1).Infof("assigned host device to host NUMA %d (guest NUMA %d) topology %q bus %#02x",
				key.hostNUMANode, key.guestNUMANode, key.topology, rootPort.controllerIndex)
		}
	}
}

func newNUMAPCIPlanner(domain *api.Domain) *numaPCIPlanner {
	planner := &numaPCIPlanner{
		domain:          domain,
		nextPXBSlot:     defaultPXBSlot,
		usedRootSlots:   map[int]struct{}{},
		usedNUMAChassis: map[int]struct{}{},
		pxbs:            map[deviceGroupKey]*pxbInfo{},
		nextChassis:     1,
	}

	maxIndex := -1
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
	}

	planner.nextControllerIndex = maxIndex + 1
	if planner.nextPXBSlot > maxRootBusSlot {
		planner.nextPXBSlot = maxRootBusSlot
	}
	return planner
}

func (p *numaPCIPlanner) ensurePXBForGroup(hostNUMA, guestNUMA int, topology string) (*pxbInfo, error) {
	key := deviceGroupKey{
		hostNUMANode:  hostNUMA,
		guestNUMANode: guestNUMA,
		topology:      topology,
	}
	if existing, ok := p.pxbs[key]; ok {
		return existing, nil
	}
	index := p.nextControllerIndex
	p.nextControllerIndex++

	slot := p.allocateRootSlot()
	if slot < 0 {
		return nil, fmt.Errorf("no slots available for PCI expander bus")
	}

	nodeVal := guestNUMA
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

	p.domain.Spec.Devices.Controllers = append(p.domain.Spec.Devices.Controllers, controller)

	info := &pxbInfo{
		index:         index,
		nextPortSlot:  0,
		nextPortValue: 0,
	}
	p.pxbs[key] = info
	return info, nil
}

func (p *numaPCIPlanner) addRootPort(info *pxbInfo) (*rootPortInfo, error) {
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
// guest NUMA node. This happens when CPU NUMA passthrough is enabled (already validated by the caller),
// the converter produced only one guest NUMA cell, and every host device being considered already
// resides on the unified group (node 0). In all other cases we preserve the original host NUMA
// placement to avoid breaking locality expectations from VFIO / IOMMU mappings.
func shouldCollapseHostDeviceNUMA(vmi *v1.VirtualMachineInstance, domain *api.Domain, hostNUMANodes map[int]struct{}) bool {
	if vmi == nil || domain == nil {
		return false
	}

	if domain.Spec.CPU.NUMA == nil {
		return false
	}

	if len(domain.Spec.CPU.NUMA.Cells) > 1 {
		return false
	}

	if len(hostNUMANodes) != 1 {
		return false
	}

	_, onlyNodeZero := hostNUMANodes[unifiedNUMAGroup]
	return onlyNodeZero
}

func getGuestNUMANodes(domain *api.Domain) map[int]struct{} {
	result := make(map[int]struct{})
	if domain == nil || domain.Spec.CPU.NUMA == nil {
		return result
	}
	for _, cell := range domain.Spec.CPU.NUMA.Cells {
		if cell.ID == "" {
			continue
		}
		if id, err := strconv.Atoi(cell.ID); err == nil {
			result[id] = struct{}{}
		}
	}
	return result
}

func mapHostToGuestNUMANode(hostNode int, guestNodes map[int]struct{}) (int, bool) {
	if len(guestNodes) == 0 {
		return hostNode, false
	}
	if _, ok := guestNodes[hostNode]; ok {
		return hostNode, true
	}
	// Fallback to the lowest guest NUMA node to keep the domain XML valid
	guestList := make([]int, 0, len(guestNodes))
	for node := range guestNodes {
		guestList = append(guestList, node)
	}
	slices.Sort(guestList)
	return guestList[0], false
}
