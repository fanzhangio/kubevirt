package hostdevice

import (
	"fmt"
	"strings"
	"testing"

	v1 "kubevirt.io/api/core/v1"

	"kubevirt.io/kubevirt/pkg/util/hardware"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
)

func TestApplyNUMAHostDeviceTopologyDisabled(t *testing.T) {
	defer restoreNUMAHelpers()
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		t.Fatal("GetDeviceNumaNodeInt should not be called when feature disabled")
		return -1, nil
	}

	vmi := &v1.VirtualMachineInstance{}
	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged, got %d", len(domain.Spec.Devices.Controllers))
	}
	if domain.Spec.Devices.HostDevices[0].Address != nil {
		t.Fatalf("expected host device address to remain unset when feature disabled")
	}
}

func TestApplyNUMAHostDeviceTopologyCreatesPXBs(t *testing.T) {
	defer restoreNUMAHelpers()

	getDeviceNumaNodeIntFunc = func(bdf string) (int, error) {
		switch bdf {
		case "0000:01:00.0":
			return 0, nil
		case "0000:02:00.0":
			return 1, nil
		default:
			return -1, nil
		}
	}
	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		return "0000:" + strings.TrimPrefix(addr.Bus, "0x") + ":" + strings.TrimPrefix(addr.Slot, "0x") + ".0", nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x02",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	var pxbCount int
	var numaNodes []int
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-expander-bus" {
			pxbCount++
			if ctrl.Target == nil || ctrl.Target.Node == nil {
				t.Fatalf("expected PXB controller to have target node")
			}
			numaNodes = append(numaNodes, *ctrl.Target.Node)
		}
	}
	if pxbCount != 2 {
		t.Fatalf("expected two expander buses, got %d", pxbCount)
	}
	if !(containsInt(numaNodes, 0) && containsInt(numaNodes, 1)) {
		t.Fatalf("expected expander buses for NUMA nodes 0 and 1, got %v", numaNodes)
	}

	rootPortBuses := make(map[string]struct{})
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-root-port" && ctrl.Address != nil {
			rootPortBuses[ctrl.Address.Bus] = struct{}{}
		}
	}

	for i, dev := range domain.Spec.Devices.HostDevices {
		if dev.Address == nil {
			t.Fatalf("expected host device %d to have an address assigned", i)
		}
		if _, found := rootPortBuses[dev.Address.Bus]; !found {
			t.Fatalf("expected host device %d to be attached to one of the NUMA root ports, got bus %s", i, dev.Address.Bus)
		}
	}
}

func TestApplyNUMAHostDeviceTopologyHandlesMdev(t *testing.T) {
	defer restoreNUMAHelpers()

	getMdevParentPCIAddressFunc = func(uuid string) (string, error) {
		if uuid != "mdev-uuid" {
			t.Fatalf("unexpected mdev uuid %s", uuid)
		}
		return "0000:03:00.0", nil
	}
	getDeviceNumaNodeIntFunc = func(bdf string) (int, error) {
		if bdf == "0000:03:00.0" {
			return 0, nil
		}
		return -1, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDeviceMDev,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								UUID: "mdev-uuid",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	if len(domain.Spec.Devices.Controllers) < 2 {
		t.Fatalf("expected expander bus created for mdev device")
	}
	if domain.Spec.Devices.HostDevices[0].Address == nil {
		t.Fatalf("expected host dev address assigned for mdev device")
	}
	if domain.Spec.Devices.HostDevices[0].Source.Address.UUID != "mdev-uuid" {
		t.Fatalf("expected source address UUID to remain unchanged")
	}
}

func restoreNUMAHelpers() {
	formatPCIAddressFunc = hardware.FormatPCIAddress
	getDeviceNumaNodeIntFunc = hardware.GetDeviceNumaNodeInt
	getMdevParentPCIAddressFunc = hardware.GetMdevParentPCIAddress
}

func containsInt(list []int, value int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

// Error Handling Tests

func TestApplyNUMAHostDeviceTopologyDeviceResolutionFailure(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		return "", fmt.Errorf("device resolution failed")
	}
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		t.Fatal("GetDeviceNumaNodeInt should not be called when device resolution fails")
		return -1, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create any new controllers when device resolution fails
	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged when device resolution fails, got %d", len(domain.Spec.Devices.Controllers))
	}
	if domain.Spec.Devices.HostDevices[0].Address != nil {
		t.Fatalf("expected host device address to remain unset when device resolution fails")
	}
}

func TestApplyNUMAHostDeviceTopologyNumaDetectionFailure(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		return "0000:01:00.0", nil
	}
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		return -1, fmt.Errorf("NUMA detection failed")
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create any new controllers when NUMA detection fails
	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged when NUMA detection fails, got %d", len(domain.Spec.Devices.Controllers))
	}
	if domain.Spec.Devices.HostDevices[0].Address != nil {
		t.Fatalf("expected host device address to remain unset when NUMA detection fails")
	}
}

func TestApplyNUMAHostDeviceTopologyMdevParentResolutionFailure(t *testing.T) {
	defer restoreNUMAHelpers()

	getMdevParentPCIAddressFunc = func(uuid string) (string, error) {
		return "", fmt.Errorf("mdev parent resolution failed")
	}
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		t.Fatal("GetDeviceNumaNodeInt should not be called when mdev parent resolution fails")
		return -1, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDeviceMDev,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								UUID: "mdev-uuid",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create any new controllers when mdev parent resolution fails
	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged when mdev parent resolution fails, got %d", len(domain.Spec.Devices.Controllers))
	}
	if domain.Spec.Devices.HostDevices[0].Address != nil {
		t.Fatalf("expected host device address to remain unset when mdev parent resolution fails")
	}
}

// Edge Case Tests

func TestApplyNUMAHostDeviceTopologyNoHostDevices(t *testing.T) {
	defer restoreNUMAHelpers()

	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		t.Fatal("GetDeviceNumaNodeInt should not be called when no host devices")
		return -1, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create any new controllers when no host devices
	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged when no host devices, got %d", len(domain.Spec.Devices.Controllers))
	}
}

func TestApplyNUMAHostDeviceTopologyNoNumaAffinity(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		return "0000:01:00.0", nil
	}
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		return -1, nil // No NUMA affinity
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create any new controllers when devices have no NUMA affinity
	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged when devices have no NUMA affinity, got %d", len(domain.Spec.Devices.Controllers))
	}
	if domain.Spec.Devices.HostDevices[0].Address != nil {
		t.Fatalf("expected host device address to remain unset when devices have no NUMA affinity")
	}
}

func TestApplyNUMAHostDeviceTopologyUnsupportedDeviceTypes(t *testing.T) {
	defer restoreNUMAHelpers()

	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		t.Fatal("GetDeviceNumaNodeInt should not be called for unsupported device types")
		return -1, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDeviceUSB, // Unsupported device type
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:   api.AddressPCI,
								Bus:    "1",
								Device: "2",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create any new controllers for unsupported device types
	if len(domain.Spec.Devices.Controllers) != 1 {
		t.Fatalf("expected controllers unchanged for unsupported device types, got %d", len(domain.Spec.Devices.Controllers))
	}
}

// Slot Allocation Tests

func TestApplyNUMAHostDeviceTopologySlotExhaustion(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		return "0000:01:00.0", nil
	}
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		return 0, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	// Create domain with all root bus slots occupied (0x0a to 0x1f)
	controllers := []api.Controller{
		{Type: "pci", Index: "0", Model: "pcie-root"},
	}

	// Fill all available slots
	for slot := 0x0a; slot <= 0x1f; slot++ {
		controllers = append(controllers, api.Controller{
			Type:  "pci",
			Index: fmt.Sprintf("%d", slot),
			Model: "pcie-expander-bus",
			Address: &api.Address{
				Type:     api.AddressPCI,
				Domain:   "0x0000",
				Bus:      "0x00",
				Slot:     fmt.Sprintf("0x%02x", slot),
				Function: "0x0",
			},
		})
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: controllers,
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should not create new controllers when slots are exhausted
	// Original controllers + all occupied slots = expected count
	expectedControllers := len(controllers)
	if len(domain.Spec.Devices.Controllers) != expectedControllers {
		t.Fatalf("expected controllers unchanged when slots exhausted, got %d, expected %d",
			len(domain.Spec.Devices.Controllers), expectedControllers)
	}
}

func TestApplyNUMAHostDeviceTopologyExistingControllerSlots(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		return "0000:01:00.0", nil
	}
	getDeviceNumaNodeIntFunc = func(string) (int, error) {
		return 0, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	// Create domain with existing controller using slot 0x0a
	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
					{
						Type:  "pci",
						Index: "1",
						Model: "pcie-expander-bus",
						Address: &api.Address{
							Type:     api.AddressPCI,
							Domain:   "0x0000",
							Bus:      "0x00",
							Slot:     "0x0a", // Using default slot
							Function: "0x0",
						},
					},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should create new PXB using next available slot (0x0b)
	var newPXBFound bool
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-expander-bus" && ctrl.Address != nil {
			if ctrl.Address.Slot == "0x0b" {
				newPXBFound = true
				break
			}
		}
	}
	if !newPXBFound {
		t.Fatalf("expected new PXB to use slot 0x0b, but found different slot")
	}
}

func TestApplyNUMAHostDeviceTopologyMultipleDevicesPerNode(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		bus := strings.TrimPrefix(addr.Bus, "0x")
		slot := strings.TrimPrefix(addr.Slot, "0x")
		return fmt.Sprintf("0000:%s:%s.0", bus, slot), nil
	}
	getDeviceNumaNodeIntFunc = func(bdf string) (int, error) {
		// Simulate 7 devices on NUMA node 0, 7 devices on NUMA node 1
		// NUMA node 0: 0000:03, 0000:04, 0000:05, 0000:06, 0000:07, 0000:08, 0000:41
		// NUMA node 1: 0000:83, 0000:84, 0000:85, 0000:86, 0000:87, 0000:88, 0000:89
		switch {
		case strings.HasPrefix(bdf, "0000:03:") || strings.HasPrefix(bdf, "0000:04:") ||
			strings.HasPrefix(bdf, "0000:05:") || strings.HasPrefix(bdf, "0000:06:") ||
			strings.HasPrefix(bdf, "0000:07:") || strings.HasPrefix(bdf, "0000:08:") ||
			strings.HasPrefix(bdf, "0000:41:"):
			return 0, nil
		case strings.HasPrefix(bdf, "0000:83:") || strings.HasPrefix(bdf, "0000:84:") ||
			strings.HasPrefix(bdf, "0000:85:") || strings.HasPrefix(bdf, "0000:86:") ||
			strings.HasPrefix(bdf, "0000:87:") || strings.HasPrefix(bdf, "0000:88:") ||
			strings.HasPrefix(bdf, "0000:89:"):
			return 1, nil
		default:
			return -1, nil
		}
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	// Create 7 devices for NUMA node 0 and 7 for NUMA node 1
	hostDevices := []api.HostDevice{}

	// NUMA node 0 devices (0000:03-06, 0000:07-08, 0000:41)
	for _, bus := range []string{"0x03", "0x04", "0x05", "0x06", "0x07", "0x08", "0x41"} {
		hostDevices = append(hostDevices, api.HostDevice{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      bus,
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		})
	}

	// NUMA node 1 devices (0000:83-86, 0000:87-88, 0000:89)
	for _, bus := range []string{"0x83", "0x84", "0x85", "0x86", "0x87", "0x88", "0x89"} {
		hostDevices = append(hostDevices, api.HostDevice{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      bus,
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		})
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: hostDevices,
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Should create 2 PXB controllers (one per NUMA node)
	var pxbCount int
	var numaNodes []int
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-expander-bus" {
			pxbCount++
			if ctrl.Target != nil && ctrl.Target.Node != nil {
				numaNodes = append(numaNodes, *ctrl.Target.Node)
			}
		}
	}
	if pxbCount != 2 {
		t.Fatalf("expected 2 PXB controllers, got %d", pxbCount)
	}
	if !(containsInt(numaNodes, 0) && containsInt(numaNodes, 1)) {
		t.Fatalf("expected PXB controllers for NUMA nodes 0 and 1, got %v", numaNodes)
	}

	// Should create 7 root ports per NUMA node (14 total)
	var rootPortCount int
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-root-port" {
			rootPortCount++
		}
	}
	if rootPortCount != 14 {
		t.Fatalf("expected 14 root ports (7 per NUMA node), got %d", rootPortCount)
	}

	// All devices should have addresses assigned
	for i, dev := range domain.Spec.Devices.HostDevices {
		if dev.Address == nil {
			t.Fatalf("expected host device %d to have an address assigned", i)
		}
	}
}

// Controller Management Tests

func TestNewNUMAPCIPlannerControllerIndexCalculation(t *testing.T) {
	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
					{Type: "pci", Index: "5", Model: "pcie-expander-bus"},
					{Type: "pci", Index: "10", Model: "pcie-root-port"},
				},
			},
		},
	}

	planner := newNUMAPCIPlanner(domain)

	// Should start from maxIndex + 1 (10 + 1 = 11)
	if planner.nextControllerIndex != 11 {
		t.Fatalf("expected nextControllerIndex to be 11, got %d", planner.nextControllerIndex)
	}
}

func TestNewNUMAPCIPlannerExistingControllerDetection(t *testing.T) {
	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
					{
						Type:  "pci",
						Index: "1",
						Model: "pcie-expander-bus",
						Address: &api.Address{
							Type:     api.AddressPCI,
							Domain:   "0x0000",
							Bus:      "0x00",
							Slot:     "0x0a",
							Function: "0x0",
						},
					},
					{
						Type:  "pci",
						Index: "2",
						Model: "pcie-expander-bus",
						Address: &api.Address{
							Type:     api.AddressPCI,
							Domain:   "0x0000",
							Bus:      "0x00",
							Slot:     "0x0b",
							Function: "0x0",
						},
					},
				},
			},
		},
	}

	planner := newNUMAPCIPlanner(domain)

	// Should detect used slots 0x0a and 0x0b
	if _, used := planner.usedRootSlots[0x0a]; !used {
		t.Fatalf("expected slot 0x0a to be marked as used")
	}
	if _, used := planner.usedRootSlots[0x0b]; !used {
		t.Fatalf("expected slot 0x0b to be marked as used")
	}

	// Should set nextPXBSlot to 0x0c (0x0b + 1)
	if planner.nextPXBSlot != 0x0c {
		t.Fatalf("expected nextPXBSlot to be 0x0c, got 0x%02x", planner.nextPXBSlot)
	}
}

// Address Assignment Tests

func TestAssignHostDeviceToRootPort(t *testing.T) {
	dev := &api.HostDevice{
		Type: api.HostDevicePCI,
		Source: api.HostDeviceSource{
			Address: &api.Address{
				Type:     api.AddressPCI,
				Domain:   "0x0000",
				Bus:      "0x01",
				Slot:     "0x00",
				Function: "0x0",
			},
		},
	}

	port := &rootPortInfo{
		controllerIndex: 5,
		bus:             3,
	}

	assignHostDeviceToRootPort(dev, port)

	// Should set NUMA-specific bus, slot/function left for PCI placement
	if dev.Address == nil {
		t.Fatalf("expected device address to be set")
	}
	if dev.Address.Type != api.AddressPCI {
		t.Fatalf("expected address type to be PCI")
	}
	if dev.Address.Domain != "0x0000" {
		t.Fatalf("expected domain to be 0x0000, got %s", dev.Address.Domain)
	}
	if dev.Address.Bus != "0x03" {
		t.Fatalf("expected bus to be 0x03, got %s", dev.Address.Bus)
	}
	// Slot and function should be empty to trigger PCI placement
	if dev.Address.Slot != "" {
		t.Fatalf("expected slot to be empty for PCI placement, got %s", dev.Address.Slot)
	}
	if dev.Address.Function != "" {
		t.Fatalf("expected function to be empty for PCI placement, got %s", dev.Address.Function)
	}
}

func TestHostDeviceAddressFormat(t *testing.T) {
	dev := &api.HostDevice{
		Type: api.HostDevicePCI,
		Source: api.HostDeviceSource{
			Address: &api.Address{
				Type:     api.AddressPCI,
				Domain:   "0x0000",
				Bus:      "0x01",
				Slot:     "0x00",
				Function: "0x0",
			},
		},
	}

	port := &rootPortInfo{
		controllerIndex: 10,
		bus:             5,
	}

	assignHostDeviceToRootPort(dev, port)

	// Should use correct hex formatting for bus, slot/function empty for PCI placement
	if dev.Address.Bus != "0x05" {
		t.Fatalf("expected bus to be formatted as 0x05, got %s", dev.Address.Bus)
	}
	if dev.Address.Slot != "" {
		t.Fatalf("expected slot to be empty for PCI placement, got %s", dev.Address.Slot)
	}
	if dev.Address.Function != "" {
		t.Fatalf("expected function to be empty for PCI placement, got %s", dev.Address.Function)
	}
}

// Real-world scenario test based on provided device examples

func TestApplyNUMAHostDeviceTopologyRealWorldScenario(t *testing.T) {
	defer restoreNUMAHelpers()

	// Mock the hardware functions to return the exact NUMA nodes from the provided examples
	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		domain := strings.TrimPrefix(addr.Domain, "0x")
		bus := strings.TrimPrefix(addr.Bus, "0x")
		slot := strings.TrimPrefix(addr.Slot, "0x")
		function := strings.TrimPrefix(addr.Function, "0x")
		return fmt.Sprintf("%s:%s:%s.%s", domain, bus, slot, function), nil
	}

	getDeviceNumaNodeIntFunc = func(bdf string) (int, error) {
		// Map the exact BDFs from the provided examples to their NUMA nodes
		switch bdf {
		// NUMA Node 0 devices
		case "0000:03:00.0", "0000:04:00.0", "0000:05:00.0", "0000:06:00.0": // NVIDIA GPUs
			return 0, nil
		case "0000:07:00.0", "0000:08:00.0": // Mellanox IB devices
			return 0, nil
		case "0000:41:00.0": // Ethernet device
			return 0, nil
		// NUMA Node 1 devices
		case "0000:83:00.0", "0000:84:00.0", "0000:85:00.0", "0000:86:00.0": // NVIDIA GPUs
			return 1, nil
		case "0000:87:00.0", "0000:88:00.0": // Mellanox IB devices
			return 1, nil
		case "0000:89:00.0": // Additional device
			return 1, nil
		default:
			return -1, fmt.Errorf("unknown device BDF: %s", bdf)
		}
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	// Create host devices matching the exact BDFs from the provided examples, provided by device plugin or DRA driver
	hostDevices := []api.HostDevice{
		// NUMA Node 0 - NVIDIA GPUs
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x03",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x04",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x05",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x06",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		// NUMA Node 0 - Mellanox IB devices
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x07",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x08",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		// NUMA Node 0 - BlueField device
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x41",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		// NUMA Node 1 - NVIDIA GPUs
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x83",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x84",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x85",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x86",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		// NUMA Node 1 - Mellanox IB devices
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x87",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x88",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
		// NUMA Node 1 - Additional device
		{
			Type: api.HostDevicePCI,
			Source: api.HostDeviceSource{
				Address: &api.Address{
					Type:     api.AddressPCI,
					Domain:   "0x0000",
					Bus:      "0x89",
					Slot:     "0x00",
					Function: "0x0",
				},
			},
		},
	}

	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
				},
				HostDevices: hostDevices,
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Verify PXB controllers are created for both NUMA nodes
	var pxbCount int
	var numaNodes []int
	var pxbSlots []string
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-expander-bus" {
			pxbCount++
			if ctrl.Target != nil && ctrl.Target.Node != nil {
				numaNodes = append(numaNodes, *ctrl.Target.Node)
			}
			if ctrl.Address != nil {
				pxbSlots = append(pxbSlots, ctrl.Address.Slot)
			}
		}
	}

	if pxbCount != 2 {
		t.Fatalf("expected 2 PXB controllers (one per NUMA node), got %d", pxbCount)
	}
	if !(containsInt(numaNodes, 0) && containsInt(numaNodes, 1)) {
		t.Fatalf("expected PXB controllers for NUMA nodes 0 and 1, got %v", numaNodes)
	}

	// Verify root ports are created (7 devices per NUMA node = 14 total root ports)
	var rootPortCount int
	var rootPortBuses = make(map[string]int) // bus -> count
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Model == "pcie-root-port" {
			rootPortCount++
			if ctrl.Address != nil {
				rootPortBuses[ctrl.Address.Bus]++
			}
		}
	}

	if rootPortCount != 14 {
		t.Fatalf("expected 14 root ports (7 per NUMA node), got %d", rootPortCount)
	}

	// Verify each NUMA node has 7 root ports
	if len(rootPortBuses) != 2 {
		t.Fatalf("expected root ports on 2 different buses (one per NUMA node), got %d buses", len(rootPortBuses))
	}
	for bus, count := range rootPortBuses {
		if count != 7 {
			t.Fatalf("expected 7 root ports on bus %s, got %d", bus, count)
		}
	}

	// Verify all host devices have addresses assigned
	for i, dev := range domain.Spec.Devices.HostDevices {
		if dev.Address == nil {
			t.Fatalf("expected host device %d to have an address assigned", i)
		}
		// Verify device is assigned to a NUMA-specific bus
		if dev.Address.Bus == "0x00" {
			t.Fatalf("expected host device %d to be assigned to NUMA-specific bus, not root bus", i)
		}
	}

	// Verify device grouping by NUMA node
	// NUMA node 0 devices should be on one bus, NUMA node 1 on another
	var numa0Buses = make(map[string]bool)
	var numa1Buses = make(map[string]bool)

	for i, dev := range domain.Spec.Devices.HostDevices {
		if dev.Address != nil {
			// Determine which NUMA node this device belongs to based on original BDF
			originalBDF := fmt.Sprintf("0000:%s:00.0",
				strings.TrimPrefix(hostDevices[i].Source.Address.Bus, "0x"))

			var expectedNumaNode int
			switch originalBDF {
			case "0000:03:00.0", "0000:04:00.0", "0000:05:00.0", "0000:06:00.0",
				"0000:07:00.0", "0000:08:00.0", "0000:41:00.0":
				expectedNumaNode = 0
			case "0000:83:00.0", "0000:84:00.0", "0000:85:00.0", "0000:86:00.0",
				"0000:87:00.0", "0000:88:00.0", "0000:89:00.0":
				expectedNumaNode = 1
			default:
				t.Fatalf("unexpected device BDF: %s", originalBDF)
			}

			if expectedNumaNode == 0 {
				numa0Buses[dev.Address.Bus] = true
			} else {
				numa1Buses[dev.Address.Bus] = true
			}
		}
	}

	// Each NUMA node should have devices on a single bus
	if len(numa0Buses) != 1 {
		t.Fatalf("expected NUMA node 0 devices to be on single bus, got %d buses: %v",
			len(numa0Buses), numa0Buses)
	}
	if len(numa1Buses) != 1 {
		t.Fatalf("expected NUMA node 1 devices to be on single bus, got %d buses: %v",
			len(numa1Buses), numa1Buses)
	}

	// The two NUMA buses should be different
	var numa0Bus, numa1Bus string
	for bus := range numa0Buses {
		numa0Bus = bus
	}
	for bus := range numa1Buses {
		numa1Bus = bus
	}
	if numa0Bus == numa1Bus {
		t.Fatalf("expected different buses for NUMA nodes 0 and 1, both using bus %s", numa0Bus)
	}

	t.Logf("NUMA Node 0 devices assigned to bus: %s", numa0Bus)
	t.Logf("NUMA Node 1 devices assigned to bus: %s", numa1Bus)
	t.Logf("Total PXB controllers: %d", pxbCount)
	t.Logf("Total root ports: %d", rootPortCount)
}

// Conflict Detection Tests
func TestNUMAPCIPlannerConflictDetection(t *testing.T) {
	defer restoreNUMAHelpers()

	formatPCIAddressFunc = func(addr *api.Address) (string, error) {
		bus := strings.TrimPrefix(addr.Bus, "0x")
		slot := strings.TrimPrefix(addr.Slot, "0x")
		return fmt.Sprintf("0000:%s:%s.0", bus, slot), nil
	}
	getDeviceNumaNodeIntFunc = func(bdf string) (int, error) {
		return 0, nil
	}

	vmi := &v1.VirtualMachineInstance{
		Spec: v1.VirtualMachineInstanceSpec{
			Domain: v1.DomainSpec{
				CPU: &v1.CPU{
					NUMA: &v1.NUMA{
						GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{},
					},
				},
			},
		},
	}

	// Create domain with existing controllers that use slots and chassis
	domain := &api.Domain{
		Spec: api.DomainSpec{
			Devices: api.Devices{
				Controllers: []api.Controller{
					{Type: "pci", Index: "0", Model: "pcie-root"},
					{
						Type:  "pci",
						Index: "1",
						Model: "pcie-expander-bus",
						Address: &api.Address{
							Type:     api.AddressPCI,
							Domain:   "0x0000",
							Bus:      "0x00",
							Slot:     "0x0a", // Using default slot
							Function: "0x0",
						},
					},
					{
						Type:  "pci",
						Index: "2",
						Model: "pcie-root-port",
						Target: &api.ControllerTarget{
							Chassis: "5", // Using chassis 5
						},
					},
				},
				HostDevices: []api.HostDevice{
					{
						Type: api.HostDevicePCI,
						Source: api.HostDeviceSource{
							Address: &api.Address{
								Type:     api.AddressPCI,
								Domain:   "0x0000",
								Bus:      "0x01",
								Slot:     "0x00",
								Function: "0x0",
							},
						},
					},
				},
			},
		},
	}

	ApplyNUMAHostDeviceTopology(vmi, domain)

	// Verify no slot conflicts
	var usedSlots = make(map[string]bool)
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Address != nil && ctrl.Address.Slot != "" {
			slotKey := fmt.Sprintf("%s:%s", ctrl.Address.Bus, ctrl.Address.Slot)
			if usedSlots[slotKey] {
				t.Fatalf("slot conflict detected: %s", slotKey)
			}
			usedSlots[slotKey] = true
		}
	}

	// Verify no chassis conflicts
	var usedChassis = make(map[string]bool)
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if ctrl.Target != nil && ctrl.Target.Chassis != "" {
			if usedChassis[ctrl.Target.Chassis] {
				t.Fatalf("chassis conflict detected: %s", ctrl.Target.Chassis)
			}
			usedChassis[ctrl.Target.Chassis] = true
		}
	}

	// Verify no controller index conflicts
	var usedIndices = make(map[string]bool)
	for _, ctrl := range domain.Spec.Devices.Controllers {
		if usedIndices[ctrl.Index] {
			t.Fatalf("controller index conflict detected: %s", ctrl.Index)
		}
		usedIndices[ctrl.Index] = true
	}

	t.Logf("No slot conflicts detected")
	t.Logf("No chassis conflicts detected")
	t.Logf("No controller index conflicts detected")
}
