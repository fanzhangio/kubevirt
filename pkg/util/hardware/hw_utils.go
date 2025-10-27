/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package hardware

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	v1 "kubevirt.io/api/core/v1"

	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
)

const (
	PCI_ADDRESS_PATTERN = `^([\da-fA-F]{4}):([\da-fA-F]{2}):([\da-fA-F]{2})\.([0-7]{1})$`

	ioResourceMem      = 0x00000200
	ioResourcePrefetch = 0x00002000
	ioResourceMem64    = 0x00100000
)

var (
	mdevDevicesBasePath = "/sys/bus/mdev/devices"
	pciDevicesBasePath  = "/sys/bus/pci/devices"
)

// Parse linux cpuset into an array of ints
// See: http://man7.org/linux/man-pages/man7/cpuset.7.html#FORMATS
func ParseCPUSetLine(cpusetLine string, limit int) (cpusList []int, err error) {
	elements := strings.Split(cpusetLine, ",")
	for _, item := range elements {
		cpuRange := strings.Split(item, "-")
		// provided a range: 1-3
		if len(cpuRange) > 1 {
			start, err := strconv.Atoi(cpuRange[0])
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(cpuRange[1])
			if err != nil {
				return nil, err
			}
			// Add cpus to the list. Assuming it's a valid range.
			for cpuNum := start; cpuNum <= end; cpuNum++ {
				if cpusList, err = safeAppend(cpusList, cpuNum, limit); err != nil {
					return nil, err
				}
			}
		} else {
			cpuNum, err := strconv.Atoi(cpuRange[0])
			if err != nil {
				return nil, err
			}
			if cpusList, err = safeAppend(cpusList, cpuNum, limit); err != nil {
				return nil, err
			}
		}
	}
	return
}

func safeAppend(cpusList []int, cpu int, limit int) ([]int, error) {
	if len(cpusList) > limit {
		return nil, fmt.Errorf("rejecting expanding CPU array for safety reasons, limit is %v", limit)
	}
	return append(cpusList, cpu), nil
}

// GetNumberOfVCPUs returns number of vCPUs
// It counts sockets*cores*threads
func GetNumberOfVCPUs(cpuSpec *v1.CPU) int64 {
	vCPUs := cpuSpec.Cores
	if cpuSpec.Sockets != 0 {
		if vCPUs == 0 {
			vCPUs = cpuSpec.Sockets
		} else {
			vCPUs *= cpuSpec.Sockets
		}
	}
	if cpuSpec.Threads != 0 {
		if vCPUs == 0 {
			vCPUs = cpuSpec.Threads
		} else {
			vCPUs *= cpuSpec.Threads
		}
	}
	return int64(vCPUs)
}

// ParsePciAddress returns an array of PCI DBSF fields (domain, bus, slot, function)
func ParsePciAddress(pciAddress string) ([]string, error) {
	pciAddrRegx, err := regexp.Compile(PCI_ADDRESS_PATTERN)
	if err != nil {
		return nil, fmt.Errorf("failed to compile pci address pattern, %v", err)
	}
	res := pciAddrRegx.FindStringSubmatch(pciAddress)
	if len(res) == 0 {
		return nil, fmt.Errorf("failed to parse pci address %s", pciAddress)
	}
	return res[1:], nil
}

func FormatPCIAddress(address *api.Address) (string, error) {
	if address == nil {
		return "", fmt.Errorf("address is nil")
	}
	if address.Type != "" && address.Type != api.AddressPCI {
		return "", fmt.Errorf("address type %s is not pci", address.Type)
	}
	formatComponent := func(value string, width int) (string, error) {
		if value == "" {
			return "", fmt.Errorf("address component missing")
		}
		trimmed := strings.TrimPrefix(strings.ToLower(value), "0x")
		if len(trimmed) > width {
			return "", fmt.Errorf("address component %s exceeds width %d", trimmed, width)
		}
		return fmt.Sprintf("%0*s", width, trimmed), nil
	}

	domain, err := formatComponent(address.Domain, 4)
	if err != nil {
		return "", err
	}
	bus, err := formatComponent(address.Bus, 2)
	if err != nil {
		return "", err
	}
	slot, err := formatComponent(address.Slot, 2)
	if err != nil {
		return "", err
	}
	function, err := formatComponent(address.Function, 1)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s:%s.%s", domain, bus, slot, function), nil
}

func GetMdevParentPCIAddress(mdevUUID string) (string, error) {
	if mdevUUID == "" {
		return "", fmt.Errorf("mdev uuid is empty")
	}
	mdevTypePath := filepath.Join(mdevDevicesBasePath, mdevUUID, "mdev_type")
	resolvedPath, err := filepath.EvalSymlinks(mdevTypePath)
	if err != nil {
		return "", err
	}
	supportedTypesDir := filepath.Dir(resolvedPath)
	deviceDir := filepath.Dir(supportedTypesDir)
	parentDevice := filepath.Base(deviceDir)

	return canonicalizePCIAddress(parentDevice)
}

func GetDeviceNumaNode(pciAddress string) (*uint32, error) {
	numaNode, err := GetDeviceNumaNodeInt(pciAddress)
	if err != nil {
		return nil, err
	}
	if numaNode < 0 {
		return nil, fmt.Errorf("numa node information unavailable for device %s", pciAddress)
	}
	value := uint32(numaNode)
	return &value, nil
}

func GetDeviceNumaNodeInt(pciAddress string) (int, error) {
	numaNodePath := filepath.Join(pciDevicesBasePath, pciAddress, "numa_node")
	// #nosec No risk for path injection. Reading static path of NUMA node info
	numaNodeStr, err := os.ReadFile(numaNodePath)
	if err != nil {
		return -1, err
	}
	numaNodeStr = bytes.TrimSpace(numaNodeStr)
	numaNodeInt, err := strconv.Atoi(string(numaNodeStr))
	if err != nil {
		return -1, err
	}
	return numaNodeInt, nil
}

func GetDevicePrefetchable64Size(pciAddress string) (uint64, error) {
	resourcePath := filepath.Join(pciDevicesBasePath, pciAddress, "resource")
	file, err := os.Open(resourcePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var total uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}

		start, err := strconv.ParseUint(strings.TrimPrefix(fields[0], "0x"), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse resource start for %s: %w", pciAddress, err)
		}
		end, err := strconv.ParseUint(strings.TrimPrefix(fields[1], "0x"), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse resource end for %s: %w", pciAddress, err)
		}
		flags, err := strconv.ParseUint(strings.TrimPrefix(fields[2], "0x"), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse resource flags for %s: %w", pciAddress, err)
		}

		if flags&ioResourceMem == 0 {
			continue
		}
		if flags&ioResourcePrefetch == 0 {
			continue
		}
		if flags&ioResourceMem64 == 0 {
			continue
		}
		if end < start {
			continue
		}

		total += (end - start + 1)
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return total, nil
}

func GetDeviceAlignedCPUs(pciAddress string) ([]int, error) {
	numaNode, err := GetDeviceNumaNode(pciAddress)
	if err != nil {
		return nil, err
	}
	cpuList, err := GetNumaNodeCPUList(int(*numaNode))
	if err != nil {
		return nil, err
	}
	return cpuList, err
}

func GetNumaNodeCPUList(numaNode int) ([]int, error) {
	filePath := fmt.Sprintf("/sys/bus/node/devices/node%d/cpulist", numaNode)
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	content = bytes.TrimSpace(content)
	cpusList, err := ParseCPUSetLine(string(content[:]), 50000)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cpulist file: %v", err)
	}

	return cpusList, nil
}

func canonicalizePCIAddress(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("empty pci address candidate")
	}

	segments, err := ParsePciAddress(value)
	if err != nil {
		return "", err
	}

	domain, err := strconv.ParseUint(segments[0], 16, 16)
	if err != nil {
		return "", err
	}
	bus, err := strconv.ParseUint(segments[1], 16, 8)
	if err != nil {
		return "", err
	}
	slot, err := strconv.ParseUint(segments[2], 16, 8)
	if err != nil {
		return "", err
	}
	function, err := strconv.ParseUint(segments[3], 16, 4)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%04x:%02x:%02x.%1x", domain, bus, slot, function), nil
}

func LookupDeviceVCPUAffinity(pciAddress string, domainSpec *api.DomainSpec) ([]uint32, error) {
	alignedVCPUList := []uint32{}
	p2vCPUMap := make(map[string]uint32)
	alignedPhysicalCPUs, err := GetDeviceAlignedCPUs(pciAddress)
	if err != nil {
		return nil, err
	}

	// make sure that the VMI has cpus from this numa node.
	cpuTune := domainSpec.CPUTune.VCPUPin
	for _, vcpuPin := range cpuTune {
		p2vCPUMap[vcpuPin.CPUSet] = vcpuPin.VCPU
	}

	for _, pcpu := range alignedPhysicalCPUs {
		if vCPU, exist := p2vCPUMap[strconv.Itoa(int(pcpu))]; exist {
			alignedVCPUList = append(alignedVCPUList, uint32(vCPU))
		}
	}
	return alignedVCPUList, nil
}
