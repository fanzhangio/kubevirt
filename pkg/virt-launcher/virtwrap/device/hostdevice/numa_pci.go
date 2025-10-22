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
	rootBusDomain   = "0x0000"
	defaultPXBSlot  = 0x0a
	maxRootBusSlot  = 0x1f
	maxRootPortSlot = 0x1f
)

var (
	formatPCIAddressFunc        = hardware.FormatPCIAddress
	getDeviceNumaNodeIntFunc    = hardware.GetDeviceNumaNodeInt
	getMdevParentPCIAddressFunc = hardware.GetMdevParentPCIAddress
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
}

type rootPortInfo struct {
	controllerIndex int
	bus             int
}

func ApplyNUMAHostDeviceTopology(vmi *v1.VirtualMachineInstance, domain *api.Domain) {
	log.Log.V(1).Info("NUMA PCI: starting NUMA host device topology application")

	if vmi == nil || domain == nil {
		log.Log.V(1).Info("NUMA PCI: VMI or domain is nil, skipping NUMA topology")
		return
	}

	// Host devcies NUMA passthrough only works when CPU NUMA passthrough is enabled
	// because without CPU NUMA passthrough, there will be only one NUMA node in the guest
	// so all host devices will be assigned to the same NUMA node.
	if vmi.Spec.Domain.CPU == nil {
		log.Log.V(1).Info("NUMA PCI: VMI CPU spec is nil, skipping NUMA topology")
		return
	}
	if vmi.Spec.Domain.CPU.NUMA == nil {
		log.Log.V(1).Info("NUMA PCI: VMI CPU NUMA spec is nil, skipping NUMA topology")
		return
	}
	if vmi.Spec.Domain.CPU.NUMA.GuestMappingPassthrough == nil {
		log.Log.V(1).Info("NUMA PCI: VMI CPU NUMA GuestMappingPassthrough is nil, skipping NUMA topology")
		return
	}

	hostDevices := domain.Spec.Devices.HostDevices
	if len(hostDevices) == 0 {
		log.Log.V(1).Info("NUMA PCI: no host devices found, skipping NUMA topology")
		return
	}

	log.Log.V(1).Infof("NUMA PCI: processing %d host devices for NUMA topology", len(hostDevices))

	planner := newNUMAPCIPlanner(domain)
	grouped := make(map[int][]*api.HostDevice)

	for i := range hostDevices {
		dev := &domain.Spec.Devices.HostDevices[i]
		aliasStr := ""
		if dev.Alias != nil {
			aliasStr = dev.Alias.GetName()
		}
		log.Log.V(1).Infof("NUMA PCI: processing device %d/%d - type=%s, alias=%s", i+1, len(hostDevices), dev.Type, aliasStr)

		if dev.Type != api.HostDevicePCI && dev.Type != api.HostDeviceMDev {
			log.Log.V(1).Infof("NUMA PCI: skipping non-PCI device type %s (alias=%s)", dev.Type, aliasStr)
			continue
		}

		bdf, err := resolveHostDevicePCIAddress(dev)
		if err != nil {
			log.Log.V(1).Reason(err).Infof("NUMA PCI: unable to resolve host device PCI address for device type=%s, alias=%s, skipping", dev.Type, aliasStr)
			continue
		}
		log.Log.V(1).Infof("NUMA PCI: resolved PCI address %s for device type=%s, alias=%s", bdf, dev.Type, aliasStr)

		numaNode, err := getDeviceNumaNodeIntFunc(bdf)
		if err != nil {
			log.Log.V(1).Infof("NUMA PCI: failed to get NUMA node for %s (alias=%s): %v, skipping", bdf, aliasStr, err)
			continue
		}
		if numaNode < 0 {
			log.Log.V(1).Infof("NUMA PCI: invalid NUMA node %d for %s (alias=%s), skipping", numaNode, bdf, aliasStr)
			continue
		}

		log.Log.V(1).Infof("NUMA PCI: device %s (alias=%s) assigned to NUMA node %d", bdf, aliasStr, numaNode)
		grouped[numaNode] = append(grouped[numaNode], dev)
	}

	if len(grouped) == 0 {
		log.Log.V(1).Info("NUMA PCI: no devices grouped by NUMA node, skipping NUMA topology")
		return
	}

	log.Log.V(1).Infof("NUMA PCI: grouped %d devices across %d NUMA nodes", len(hostDevices), len(grouped))
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
		log.Log.V(4).Infof("NUMA PCI: creating PXB for NUMA node %d with %d devices", numaNode, len(devices))
		pxb, err := planner.ensurePXBForNUMA(numaNode)
		if err != nil {
			log.Log.Reason(err).Errorf("failed to create PCI expander bus for NUMA node %d", numaNode)
			continue
		}
		log.Log.V(4).Infof("NUMA PCI: created PXB for NUMA node %d", numaNode)
		for _, dev := range devices {
			rootPort, err := planner.addRootPort(pxb)
			if err != nil {
				log.Log.Reason(err).Error("failed to allocate root port for host device")
				continue
			}
			devAliasStr := ""
			if dev.Alias != nil {
				devAliasStr = dev.Alias.GetName()
			}
			log.Log.V(1).Infof("NUMA PCI: assigned device %s (alias=%s) to root port on bus 0x%02x",
				dev.Source.Address.Bus+":"+dev.Source.Address.Slot+":"+dev.Source.Address.Function, devAliasStr, rootPort.bus)
			assignHostDeviceToRootPort(dev, rootPort)
		}
	}

	log.Log.V(1).Infof("NUMA PCI: completed NUMA host device topology application - created PXBs for %d NUMA nodes", len(numaNodes))
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

func (p *numaPCIPlanner) ensurePXBForNUMA(numa int) (*pxbInfo, error) {
	if existing, ok := p.pxbs[numa]; ok {
		return existing, nil
	}
	index := p.nextControllerIndex
	p.nextControllerIndex++

	slot := p.allocateRootSlot()
	if slot < 0 {
		return nil, fmt.Errorf("no slots available for PCI expander bus")
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

	p.domain.Spec.Devices.Controllers = append(p.domain.Spec.Devices.Controllers, controller)

	info := &pxbInfo{
		index:         index,
		nextPortSlot:  0,
		nextPortValue: 0,
	}
	p.pxbs[numa] = info
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
		bus:             info.index,
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

func assignHostDeviceToRootPort(dev *api.HostDevice, port *rootPortInfo) {
	if dev.Address == nil {
		dev.Address = &api.Address{}
	}
	// Set NUMA-specific bus, let PCI placement handle slot assignment
	dev.Address = &api.Address{
		Type:   api.AddressPCI,
		Domain: rootBusDomain,
		Bus:    fmt.Sprintf("0x%02x", port.bus),
		// Slot and Function left empty to trigger PCI placement logic
		Slot:     "",
		Function: "",
	}
}

func resolveHostDevicePCIAddress(dev *api.HostDevice) (string, error) {
	if dev == nil {
		log.Log.V(1).Info("NUMA PCI: host device is nil")
		return "", fmt.Errorf("host device is nil")
	}
	aliasStr := ""
	if dev.Alias != nil {
		aliasStr = dev.Alias.GetName()
	}

	if dev.Source.Address == nil {
		log.Log.V(1).Infof("NUMA PCI: host device source address is nil for device type=%s, alias=%s", dev.Type, aliasStr)
		return "", fmt.Errorf("host device source address is nil")
	}

	addr := dev.Source.Address
	log.Log.V(1).Infof("NUMA PCI: resolving address for device type=%s, alias=%s - UUID=%s, Domain=%s, Bus=%s, Slot=%s, Function=%s",
		dev.Type, aliasStr, addr.UUID, addr.Domain, addr.Bus, addr.Slot, addr.Function)

	// Check for MDEV devices first
	if addr.UUID != "" {
		log.Log.V(1).Infof("NUMA PCI: processing MDEV device with UUID=%s", addr.UUID)
		result, err := getMdevParentPCIAddressFunc(addr.UUID)
		if err != nil {
			log.Log.V(1).Infof("NUMA PCI: failed to get MDEV parent PCI address for UUID=%s: %v", addr.UUID, err)
		} else {
			log.Log.V(1).Infof("NUMA PCI: MDEV parent PCI address resolved: %s", result)
		}
		return result, err
	}

	// Check for PCI devices with complete address
	if addr.Domain != "" && addr.Bus != "" && addr.Slot != "" && addr.Function != "" {
		log.Log.V(1).Infof("NUMA PCI: processing PCI device with complete address")
		result, err := formatPCIAddressFunc(addr)
		if err != nil {
			log.Log.V(1).Infof("NUMA PCI: failed to format PCI address - Domain=%s, Bus=%s, Slot=%s, Function=%s: %v",
				addr.Domain, addr.Bus, addr.Slot, addr.Function, err)
		} else {
			log.Log.V(1).Infof("NUMA PCI: PCI address formatted successfully: %s", result)
		}
		return result, err
	}

	// Log detailed information about why the address format is unsupported
	log.Log.V(1).Infof("NUMA PCI: unsupported address format for device type=%s, alias=%s", dev.Type, aliasStr)
	log.Log.V(1).Infof("NUMA PCI: address components - UUID=%s, Domain=%s, Bus=%s, Slot=%s, Function=%s",
		addr.UUID, addr.Domain, addr.Bus, addr.Slot, addr.Function)

	// Check which components are missing
	missing := []string{}
	if addr.UUID == "" && addr.Domain == "" {
		missing = append(missing, "UUID or Domain")
	}
	if addr.Bus == "" {
		missing = append(missing, "Bus")
	}
	if addr.Slot == "" {
		missing = append(missing, "Slot")
	}
	if addr.Function == "" {
		missing = append(missing, "Function")
	}

	if len(missing) > 0 {
		log.Log.V(1).Infof("NUMA PCI: missing address components: %v", missing)
	}

	return "", fmt.Errorf("unsupported host device address format - missing components: %v", missing)
}
