package hostdevice

import (
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
