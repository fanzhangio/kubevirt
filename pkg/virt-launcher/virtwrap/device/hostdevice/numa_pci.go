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
)

var (
	formatPCIAddressFunc           = hardware.FormatPCIAddress
	getDeviceNumaNodeIntFunc       = hardware.GetDeviceNumaNodeInt
	getMdevParentPCIAddressFunc    = hardware.GetMdevParentPCIAddress
	getDevicePCIProximityGroupFunc = hardware.GetDevicePCITopologyGroup
	getDevicePCIPathHierarchyFunc  = hardware.GetDevicePCIPathHierarchy
	getDeviceIOMMUGroupInfoFunc    = hardware.GetDeviceIOMMUGroupInfo
)

type numaPCIPlanner struct {
	domain              *api.Domain
	nextControllerIndex int
	nextChassis         int
	nextPXBSlot         int
	usedRootSlots       map[int]struct{}
	usedNUMAChassis     map[int]struct{}
	pxbs                map[pxbKey]*pxbInfo
	rootPorts           map[deviceGroupKey]*rootPortInfo
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
	dev             *api.HostDevice
	hostNUMANode    int
	guestNUMANode   int
	bdf             string
	topologyGroup   string
	path            []string
	pathKey         string
	iommuGroup      int
	iommuPeers      []string
	mappingAccurate bool
}

type deviceGroupKey struct {
	guestNUMANode int
	hostNUMANode  int
	pathKey       string
}

type pxbKey struct {
	guestNUMANode int
	hostNUMANode  int
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
	hostToGuestNUMA := getHostToGuestNUMAMap(domain)
	unmappedHostNodes := make(map[int]struct{})
	allMappingsAccurate := true

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
		topologyGroup, err := getDevicePCIProximityGroupFunc(bdf)
		if err != nil {
			log.Log.V(1).Reason(err).Infof("using default topology grouping for host device %s (host NUMA %d)", bdf, numaNode)
			topologyGroup = fmt.Sprintf("numa-%d", numaNode)
		}

		path, err := getDevicePCIPathHierarchyFunc(bdf)
		if err != nil {
			log.Log.V(1).Reason(err).Infof("skipping host device %s - unable to derive PCI hierarchy", bdf)
			continue
		}
		pathKey := bdf
		if len(path) > 1 {
			pathKey = strings.Join(path[:len(path)-1], "/")
		}

		iommuGroup, iommuPeers, err := getDeviceIOMMUGroupInfoFunc(bdf)
		if err != nil {
			log.Log.Reason(err).Warningf("unable to detect IOMMU group for host device %s", bdf)
		}

		guestNode, mappingAccurate := mapHostToGuestNUMANode(numaNode, guestNUMANodes, hostToGuestNUMA)
		if !mappingAccurate {
			unmappedHostNodes[numaNode] = struct{}{}
			allMappingsAccurate = false
			log.Log.V(1).Infof("host device %s (host NUMA %d) cannot be matched to a distinct guest NUMA node", bdf, numaNode)
		} else {
			log.Log.V(1).Infof("host device %s aligned with guest NUMA node %d", bdf, guestNode)
		}

		devicesWithNUMA = append(devicesWithNUMA, deviceNUMAInfo{
			dev:             dev,
			hostNUMANode:    numaNode,
			guestNUMANode:   guestNode,
			bdf:             bdf,
			topologyGroup:   topologyGroup,
			path:            path,
			pathKey:         pathKey,
			iommuGroup:      iommuGroup,
			iommuPeers:      iommuPeers,
			mappingAccurate: mappingAccurate,
		})
		hostNUMANodes[numaNode] = struct{}{}
	}

	if len(devicesWithNUMA) == 0 {
		log.Log.V(1).Info("NUMA host device topology not applied: no devices with NUMA affinity detected")
		return
	}

	if !allMappingsAccurate {
		var nodes []string
		for node := range unmappedHostNodes {
			nodes = append(nodes, strconv.Itoa(node))
		}
		slices.Sort(nodes)
		log.Log.Infof("NUMA host device topology not applied: guest NUMA topology lacks representation for host NUMA nodes %s", strings.Join(nodes, ","))
		return
	}

	if domain.Spec.CPU.NUMA != nil && len(domain.Spec.CPU.NUMA.Cells) == 1 && len(hostNUMANodes) > 1 {
		var hostNodes []string
		for node := range hostNUMANodes {
			hostNodes = append(hostNodes, strconv.Itoa(node))
		}
		slices.Sort(hostNodes)
		log.Log.Infof("NUMA host device topology not applied: guest exposes a single NUMA cell but devices span host NUMA nodes %s", strings.Join(hostNodes, ","))
		return
	}

	collapseNUMA := shouldCollapseHostDeviceNUMA(vmi, domain, hostNUMANodes)
	if collapseNUMA {
		log.Log.V(1).Info("collapsing host device NUMA groups to a single guest NUMA node")
	}

	grouped := make(map[deviceGroupKey][]deviceNUMAInfo)
	for _, info := range devicesWithNUMA {
		groupKey := deviceGroupKey{
			hostNUMANode:  info.hostNUMANode,
			guestNUMANode: info.guestNUMANode,
			pathKey:       info.pathKey,
		}
		if collapseNUMA {
			log.Log.V(1).Infof("host device %s (host NUMA %d, path %q) collapsed to guest NUMA group %d", info.bdf, info.hostNUMANode, info.pathKey, unifiedNUMAGroup)
			groupKey.hostNUMANode = unifiedNUMAGroup
			groupKey.guestNUMANode = unifiedNUMAGroup
		} else {
			log.Log.V(1).Infof("host device %s grouped to host NUMA %d (guest NUMA %d) path %q", info.bdf, groupKey.hostNUMANode, groupKey.guestNUMANode, groupKey.pathKey)
		}
		grouped[groupKey] = append(grouped[groupKey], info)
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
		if a.pathKey == b.pathKey {
			return 0
		}
		if a.pathKey < b.pathKey {
			return -1
		}
		return 1
	})

	requestedDevices := make(map[string]struct{}, len(devicesWithNUMA))
	for _, info := range devicesWithNUMA {
		requestedDevices[info.bdf] = struct{}{}
	}

	for _, key := range groupKeys {
		infos := grouped[key]
		if len(infos) == 0 {
			continue
		}

		pathLabel := infos[0].pathKey
		if len(infos[0].path) > 0 {
			pathLabel = strings.Join(infos[0].path, " -> ")
		}

		log.Log.V(1).Infof("setting up PCI expander bus for host NUMA %d (guest NUMA %d) path %q with %d devices",
			key.hostNUMANode, key.guestNUMANode, pathLabel, len(infos))

		pxb, err := planner.ensurePXB(key.hostNUMANode, key.guestNUMANode)
		if err != nil {
			log.Log.Reason(err).Errorf("failed to create PCI expander bus for host NUMA %d (guest NUMA %d)", key.hostNUMANode, key.guestNUMANode)
			continue
		}

		rootPort, err := planner.ensureRootPortForGroup(pxb, key)
		if err != nil {
			log.Log.Reason(err).Error("failed to allocate root port for host device path")
			continue
		}

		for _, info := range infos {
			if info.iommuGroup >= 0 {
				for _, peer := range info.iommuPeers {
					if peer == info.bdf {
						continue
					}
					if _, present := requestedDevices[peer]; !present {
						log.Log.Warningf("host device %s belongs to IOMMU group %d with peer %s that is not attached to the VMI", info.bdf, info.iommuGroup, peer)
					}
				}
			}

			assignHostDeviceToRootPort(info.dev, rootPort)
			log.Log.V(1).Infof("assigned host device %s to host NUMA %d (guest NUMA %d) bus %#02x", info.bdf, key.hostNUMANode, key.guestNUMANode, rootPort.controllerIndex)
		}
	}
}

func newNUMAPCIPlanner(domain *api.Domain) *numaPCIPlanner {
	planner := &numaPCIPlanner{
		domain:          domain,
		nextPXBSlot:     defaultPXBSlot,
		usedRootSlots:   map[int]struct{}{},
		usedNUMAChassis: map[int]struct{}{},
		pxbs:            map[pxbKey]*pxbInfo{},
		rootPorts:       map[deviceGroupKey]*rootPortInfo{},
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

func (p *numaPCIPlanner) ensurePXB(hostNUMA, guestNUMA int) (*pxbInfo, error) {
	key := pxbKey{
		hostNUMANode:  hostNUMA,
		guestNUMANode: guestNUMA,
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

func (p *numaPCIPlanner) ensureRootPortForGroup(pxb *pxbInfo, key deviceGroupKey) (*rootPortInfo, error) {
	if existing, ok := p.rootPorts[key]; ok {
		return existing, nil
	}
	rootPort, err := p.addRootPort(pxb)
	if err != nil {
		return nil, err
	}
	p.rootPorts[key] = rootPort
	return rootPort, nil
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

func mapHostToGuestNUMANode(hostNode int, guestNodes map[int]struct{}, hostToGuest map[int]int) (int, bool) {
	if mapped, ok := hostToGuest[hostNode]; ok {
		return mapped, true
	}
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

func getHostToGuestNUMAMap(domain *api.Domain) map[int]int {
	result := make(map[int]int)
	if domain == nil || domain.Spec.NUMATune == nil {
		return result
	}
	for _, node := range domain.Spec.NUMATune.MemNodes {
		if strings.TrimSpace(node.NodeSet) == "" {
			continue
		}
		values, err := hardware.ParseCPUSetLine(node.NodeSet, 1024)
		if err != nil {
			log.Log.V(1).Reason(err).Infof("unable to parse NUMA memnode nodeset %q", node.NodeSet)
			continue
		}
		for _, host := range values {
			result[host] = int(node.CellID)
		}
	}
	return result
}
