package numa

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"kubevirt.io/kubevirt/pkg/virt-config/featuregate"

	"kubevirt.io/kubevirt/pkg/libvmi"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
	"kubevirt.io/kubevirt/tests/console"
	"kubevirt.io/kubevirt/tests/decorators"
	"kubevirt.io/kubevirt/tests/exec"
	"kubevirt.io/kubevirt/tests/framework/kubevirt"
	"kubevirt.io/kubevirt/tests/libdomain"
	"kubevirt.io/kubevirt/tests/libkubevirt"
	kvconfig "kubevirt.io/kubevirt/tests/libkubevirt/config"
	"kubevirt.io/kubevirt/tests/libnode"
	"kubevirt.io/kubevirt/tests/libvmifact"
	"kubevirt.io/kubevirt/tests/libwait"
	"kubevirt.io/kubevirt/tests/testsuite"
)

const (
	requiredGPUResourceAmount = 2
	fedoraCPUCores            = 4
	vmiCleanupTimeoutSeconds  = 180
	minNUMANodesForSpread     = 2
	gpuInfoFieldsExpected     = 4
	pciDomainWidth            = 4
	pciBusWidth               = 2
	pciSlotWidth              = 2
	pciFunctionWidth          = 1
	pxbControllerModel        = "pcie-expander-bus"
	waitForGPUTimeout         = 2 * time.Minute
	gpuResourcePollInterval   = 5 * time.Second
)

var _ = Describe("[sig-compute]NUMA", Serial, decorators.SigCompute, func() {
	var virtClient kubecli.KubevirtClient
	BeforeEach(func() {
		virtClient = kubevirt.Client()
	})

	It("[test_id:7299] topology should be mapped to the guest and hugepages should be allocated",
		decorators.RequiresNodeWithCPUManager, decorators.RequiresHugepages2Mi, func() {
			var err error
			cpuVMI := libvmifact.NewCirros()
			cpuVMI.Spec.Domain.Resources.Requests[k8sv1.ResourceMemory] = resource.MustParse("128Mi")
			cpuVMI.Spec.Domain.CPU = &v1.CPU{
				Cores:                 3,
				DedicatedCPUPlacement: true,
				NUMA:                  &v1.NUMA{GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{}},
			}
			cpuVMI.Spec.Domain.Memory = &v1.Memory{
				Hugepages: &v1.Hugepages{PageSize: "2Mi"},
			}

			By("Starting a VirtualMachineInstance")
			cpuVMI, err = virtClient.VirtualMachineInstance(
				testsuite.NamespaceTestDefault).Create(context.Background(), cpuVMI, metav1.CreateOptions{})
			Expect(err).ToNot(HaveOccurred())
			cpuVMI = libwait.WaitForSuccessfulVMIStart(cpuVMI)
			By("Fetching the numa memory mapping")
			handler, err := libnode.GetVirtHandlerPod(virtClient, cpuVMI.Status.NodeName)
			Expect(err).ToNot(HaveOccurred())
			pid := getQEMUPID(handler, cpuVMI)

			By("Checking if the pinned numa memory chunks match the VMI memory size")
			scanner := bufio.NewScanner(strings.NewReader(getNUMAMapping(virtClient, handler, pid)))
			rex := regexp.MustCompile(`bind:(\d+) .+memfd:.+N(\d+)=(\d+).+kernelpagesize_kB=(\d+)`)
			mappings := map[int]mapping{}
			for scanner.Scan() {
				if findings := rex.FindStringSubmatch(scanner.Text()); findings != nil {
					mappings[mustAtoi(findings[1])] = mapping{
						BindNode:           mustAtoi(findings[1]),
						AllocationNode:     mustAtoi(findings[2]),
						Pages:              mustAtoi(findings[3]),
						PageSizeAsQuantity: toKi(mustAtoi(findings[4])),
						PageSize:           mustAtoi(findings[4]),
					}
				}
			}

			sum := 0
			requestedPageSize := resource.MustParse(cpuVMI.Spec.Domain.Memory.Hugepages.PageSize)
			requestedMemory := cpuVMI.Spec.Domain.Resources.Requests[k8sv1.ResourceMemory]
			for _, m := range mappings {
				Expect(m.PageSizeAsQuantity.Equal(requestedPageSize)).To(BeTrue())
				Expect(m.BindNode).To(Equal(m.AllocationNode))
				sum += m.Pages
			}
			const memoryFactor = 2048
			Expect(resource.MustParse(fmt.Sprintf("%dKi", sum*memoryFactor)).Equal(requestedMemory)).To(BeTrue())

			By("Fetching the domain XML")
			domSpec, err := libdomain.GetRunningVMIDomainSpec(cpuVMI)
			Expect(err).ToNot(HaveOccurred())

			By("checking that we really deal with a domain with numa configured")
			Expect(domSpec.CPU.NUMA.Cells).ToNot(BeEmpty())

			By("Checking if number of memory chunkgs matches the number of nodes on the VM")
			Expect(mappings).To(HaveLen(len(domSpec.MemoryBacking.HugePages.HugePage)))
			Expect(mappings).To(HaveLen(len(domSpec.CPU.NUMA.Cells)))
			Expect(mappings).To(HaveLen(len(domSpec.NUMATune.MemNodes)))

			By("checking if the guest came up and is healthy")
			Expect(console.LoginToCirros(cpuVMI)).To(Succeed())
		})
})

func getQEMUPID(handlerPod *k8sv1.Pod, vmi *v1.VirtualMachineInstance) string {
	var stdout, stderr string
	const (
		expectedProcesses    = 2
		expectedPathElements = 4
	)
	// Using `ps` here doesn't work reliably. Grepping /proc instead.
	// The "[g]" prevents grep from finding its own process
	Eventually(func() (err error) {
		stdout, stderr, err = exec.ExecuteCommandOnPodWithResults(handlerPod, "virt-handler",
			[]string{
				"/bin/bash",
				"-c",
				fmt.Sprintf("grep -l '[g]uest=%s_%s' /proc/*/cmdline", vmi.Namespace, vmi.Name),
			})
		return err
	}, 3*time.Second, 500*time.Millisecond).Should(Succeed(), stderr, stdout)

	strs := strings.Split(stdout, "\n")
	Expect(strs).To(HaveLen(expectedProcesses),
		"more (or less?) than one matching process was found")
	path := strings.Split(strs[0], "/")
	Expect(path).To(HaveLen(expectedPathElements), "the cmdline path is invalid")

	return path[2]
}

var _ = Describe("[sig-compute][GPU]NUMA passthrough alignment", Serial, decorators.SigCompute, decorators.GPU, func() {
	var (
		virtClient      kubecli.KubevirtClient
		originalConfig  *v1.KubeVirtConfiguration
		configApplied   bool
		discoveredGPUs  []gpuDeviceInfo
		selectedResName string
	)

	BeforeEach(func() {
		virtClient = kubevirt.Client()
		kv := libkubevirt.GetCurrentKv(virtClient)
		originalConfig = kv.Spec.Configuration.DeepCopy()

		var err error
		discoveredGPUs, err = discoverHostGPUDevices(virtClient)
		if err != nil || len(discoveredGPUs) == 0 {
			Skip("no passthrough-capable NVIDIA GPUs discovered on the cluster")
		}

		err = ensureGPUHostDeviceConfiguration(virtClient, originalConfig, discoveredGPUs)
		if err != nil {
			Skip(fmt.Sprintf("failed to configure GPU host devices: %v", err))
		}
		configApplied = true

		selectedResName = discoveredGPUs[0].ResourceName
		if ok := waitForGPUResource(virtClient, selectedResName, requiredGPUResourceAmount, waitForGPUTimeout); !ok {
			Skip(fmt.Sprintf("resource %s not available or insufficient quantity", selectedResName))
		}
	})

	AfterEach(func() {
		if configApplied && originalConfig != nil {
			kvconfig.UpdateKubeVirtConfigValueAndWait(*originalConfig)
		}
	})

	It("should mirror host NUMA placement for passthrough GPUs", func() {
		if selectedResName == "" {
			Skip("no GPU resource selected")
		}

		vmi := libvmifact.NewFedora(libvmi.WithCPUCount(fedoraCPUCores, 1, 1))
		if vmi.Spec.Domain.CPU == nil {
			vmi.Spec.Domain.CPU = &v1.CPU{}
		}
		vmi.Spec.Domain.CPU.DedicatedCPUPlacement = true
		vmi.Spec.Domain.CPU.NUMA = &v1.NUMA{GuestMappingPassthrough: &v1.NUMAGuestMappingPassthrough{}}
		vmi.Spec.Domain.Resources.Requests[k8sv1.ResourceMemory] = resource.MustParse("4Gi")
		vmi.Spec.Domain.Devices.GPUs = []v1.GPU{
			{
				Name:       "passthrough0",
				DeviceName: selectedResName,
			},
			{
				Name:       "passthrough1",
				DeviceName: selectedResName,
			},
		}

		By("Starting a GPU passthrough VirtualMachineInstance")
		var err error
		vmi, err = virtClient.VirtualMachineInstance(testsuite.NamespaceTestDefault).Create(context.Background(), vmi, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		defer func() {
			_ = virtClient.VirtualMachineInstance(testsuite.NamespaceTestDefault).Delete(context.Background(), vmi.Name, metav1.DeleteOptions{})
			libwait.WaitForVirtualMachineToDisappearWithTimeout(vmi, vmiCleanupTimeoutSeconds)
		}()

		vmi = libwait.WaitForSuccessfulVMIStart(vmi)

		domSpec, err := libdomain.GetRunningVMIDomainSpec(vmi)
		Expect(err).ToNot(HaveOccurred())

		handler, err := libnode.GetVirtHandlerPod(virtClient, vmi.Status.NodeName)
		Expect(err).ToNot(HaveOccurred())

		pxbNodeByIndex := make(map[int]int)
		rootPortBusToNode := make(map[string]int)

		for _, ctrl := range domSpec.Devices.Controllers {
			if ctrl.Model == pxbControllerModel && ctrl.Target != nil && ctrl.Target.Node != nil {
				idx, err := strconv.Atoi(ctrl.Index)
				if err != nil {
					continue
				}
				pxbNodeByIndex[idx] = *ctrl.Target.Node
			}
		}

		for _, ctrl := range domSpec.Devices.Controllers {
			if ctrl.Model == "pcie-root-port" && ctrl.Address != nil {
				busVal, err := strconv.ParseInt(strings.TrimPrefix(ctrl.Address.Bus, "0x"), 16, 32)
				if err != nil {
					continue
				}
				if node, ok := pxbNodeByIndex[int(busVal)]; ok {
					rootPortBusToNode[ctrl.Address.Bus] = node
				}
			}
		}

		if len(rootPortBusToNode) == 0 {
			Skip("no NUMA-aware root ports detected in domain XML")
		}

		hostNUMAObserved := make(map[int]struct{})
		for _, hostDev := range domSpec.Devices.HostDevices {
			if hostDev.Source.Address == nil || hostDev.Address == nil {
				continue
			}
			hostBDF := formatPCIAddress(hostDev.Source.Address)
			numaNode, err := getHostDeviceNUMANode(handler, hostBDF)
			if err != nil || numaNode < 0 {
				continue
			}

			hostNUMAObserved[numaNode] = struct{}{}

			if node, ok := rootPortBusToNode[hostDev.Address.Bus]; ok {
				Expect(node).To(Equal(numaNode), fmt.Sprintf("host device %s should attach to NUMA-aware root port", hostBDF))
			} else {
				Fail(fmt.Sprintf("host device bus %s not mapped to any NUMA root port", hostDev.Address.Bus))
			}
		}

		if len(hostNUMAObserved) < minNUMANodesForSpread {
			Skip("GPUs are not distributed across different NUMA nodes on this host")
		}

		for node := range hostNUMAObserved {
			found := false
			for _, ctrl := range domSpec.Devices.Controllers {
				if ctrl.Model == pxbControllerModel && ctrl.Target != nil && ctrl.Target.Node != nil && *ctrl.Target.Node == node {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), fmt.Sprintf("no pxb-pcie controller targeting NUMA node %d found", node))
		}
	})
})

func getNUMAMapping(virtClient kubecli.KubevirtClient, pod *k8sv1.Pod, pid string) string {
	stdout, stderr, err := exec.ExecuteCommandOnPodWithResults(pod, "virt-handler",
		[]string{
			"/bin/bash",
			"-c",
			fmt.Sprintf("trap '' URG && cat /proc/%v/numa_maps", pid),
		})
	Expect(err).ToNot(HaveOccurred(), stderr)
	return stdout
}

type mapping struct {
	BindNode           int
	AllocationNode     int
	Pages              int
	PageSizeAsQuantity resource.Quantity
	PageSize           int
}

func mustAtoi(str string) int {
	i, err := strconv.Atoi(str)
	ExpectWithOffset(1, err).ToNot(HaveOccurred())
	return i
}

func toKi(value int) resource.Quantity {
	return resource.MustParse(fmt.Sprintf("%dKi", value))
}

type gpuDeviceInfo struct {
	VendorDevice string
	ResourceName string
	BDF          string
	NUMANode     int
	NodeName     string
}

func discoverHostGPUDevices(virtClient kubecli.KubevirtClient) ([]gpuDeviceInfo, error) {
	nodes, err := virtClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var devices []gpuDeviceInfo
	deviceMap := make(map[string]struct{})
	for _, node := range nodes.Items {
		handler, err := libnode.GetVirtHandlerPod(virtClient, node.Name)
		if err != nil {
			continue
		}
		script := `for dev in /sys/bus/pci/devices/*; do
  [ -e "$dev/vendor" ] || continue
  vendor=$(cat "$dev/vendor")
  if [ "$vendor" = "0x10de" ]; then
    device=$(cat "$dev/device")
    driver="none"
    if [ -L "$dev/driver" ]; then
      driver=$(basename "$(readlink "$dev/driver")")
    fi
    numa="-1"
    if [ -f "$dev/numa_node" ]; then
      numa=$(cat "$dev/numa_node")
    fi
    echo "$(basename "$dev") $device $numa $driver"
  fi
done`
		stdout, stderr, err := exec.ExecuteCommandOnPodWithResults(handler, "virt-handler", []string{"/bin/bash", "-c", script})
		if err != nil {
			if stderr != "" {
				fmt.Printf("failed to discover GPUs on node %s: %s\n", node.Name, stderr)
			}
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < gpuInfoFieldsExpected {
				continue
			}
			driver := fields[3]
			if driver != "vfio-pci" {
				continue
			}
			numa, err := strconv.Atoi(fields[2])
			if err != nil || numa < 0 {
				continue
			}
			bdf := fields[0]
			deviceID := strings.ToLower(strings.TrimPrefix(fields[1], "0x"))
			vendorDevice := fmt.Sprintf("10de:%s", deviceID)
			resourceName := fmt.Sprintf("nvidia.com/%s", strings.ReplaceAll(vendorDevice, ":", "-"))
			key := fmt.Sprintf("%s-%s", node.Name, bdf)
			if _, exists := deviceMap[key]; exists {
				continue
			}
			deviceMap[key] = struct{}{}
			devices = append(devices, gpuDeviceInfo{
				VendorDevice: vendorDevice,
				ResourceName: resourceName,
				BDF:          bdf,
				NUMANode:     numa,
				NodeName:     node.Name,
			})
		}
	}
	return devices, nil
}

func ensureGPUHostDeviceConfiguration(
	virtClient kubecli.KubevirtClient,
	original *v1.KubeVirtConfiguration,
	devices []gpuDeviceInfo,
) error {
	if len(devices) == 0 {
		return fmt.Errorf("no GPU devices provided")
	}
	cfg := original.DeepCopy()
	if cfg.DeveloperConfiguration == nil {
		cfg.DeveloperConfiguration = &v1.DeveloperConfiguration{}
	}
	hasHostDevicesGate := false
	for _, gate := range cfg.DeveloperConfiguration.FeatureGates {
		if gate == featuregate.HostDevicesGate {
			hasHostDevicesGate = true
			break
		}
	}
	if !hasHostDevicesGate {
		cfg.DeveloperConfiguration.FeatureGates = append(cfg.DeveloperConfiguration.FeatureGates, featuregate.HostDevicesGate)
	}
	if cfg.PermittedHostDevices == nil {
		cfg.PermittedHostDevices = &v1.PermittedHostDevices{}
	}
	existing := make(map[string]struct{})
	for _, pci := range cfg.PermittedHostDevices.PciHostDevices {
		existing[strings.ToLower(pci.PCIVendorSelector)] = struct{}{}
	}
	for _, dev := range devices {
		selector := strings.ToLower(dev.VendorDevice)
		if _, ok := existing[selector]; ok {
			continue
		}
		cfg.PermittedHostDevices.PciHostDevices = append(cfg.PermittedHostDevices.PciHostDevices, v1.PciHostDevice{
			PCIVendorSelector: strings.ToUpper(dev.VendorDevice),
			ResourceName:      dev.ResourceName,
		})
		existing[selector] = struct{}{}
	}
	kvconfig.UpdateKubeVirtConfigValueAndWait(*cfg)
	return nil
}

func waitForGPUResource(virtClient kubecli.KubevirtClient, resourceName string, minimum int64, timeout time.Duration) bool {
	k8sResource := k8sv1.ResourceName(resourceName)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, gpuResourcePollInterval, timeout, true, func(context.Context) (bool, error) {
		nodes, err := virtClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		var total int64
		for _, node := range nodes.Items {
			if qty, ok := node.Status.Allocatable[k8sResource]; ok {
				if val, ok := qty.AsInt64(); ok {
					total += val
				}
			}
		}
		return total >= minimum, nil
	})
	return err == nil
}

func formatPCIAddress(addr *api.Address) string {
	if addr == nil {
		return ""
	}
	trim := func(val string, width int) string {
		normalized := strings.TrimPrefix(strings.ToLower(val), "0x")
		return fmt.Sprintf("%0*s", width, normalized)
	}
	return fmt.Sprintf("%s:%s:%s.%s",
		trim(addr.Domain, pciDomainWidth),
		trim(addr.Bus, pciBusWidth),
		trim(addr.Slot, pciSlotWidth),
		trim(addr.Function, pciFunctionWidth),
	)
}

func getHostDeviceNUMANode(handlerPod *k8sv1.Pod, bdf string) (int, error) {
	cmd := fmt.Sprintf("cat /sys/bus/pci/devices/%s/numa_node", bdf)
	stdout, stderr, err := exec.ExecuteCommandOnPodWithResults(handlerPod, "virt-handler", []string{"/bin/bash", "-c", cmd})
	if err != nil {
		if stderr != "" {
			return -1, errors.New(stderr)
		}
		return -1, err
	}
	val := strings.TrimSpace(stdout)
	if val == "" {
		return -1, fmt.Errorf("empty numa node response")
	}
	numa, err := strconv.Atoi(val)
	if err != nil {
		return -1, err
	}
	return numa, nil
}
