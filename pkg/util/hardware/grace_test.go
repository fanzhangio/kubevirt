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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NVIDIA Grace PCI IDs", func() {
	DescribeTable("detects supported Grace GPU PCI IDs", func(vendorID, deviceID string, expected bool) {
		Expect(IsNVIDIAGraceGPU(vendorID, deviceID)).To(Equal(expected))
	},
		Entry("Grace GPU", "10DE", "2342", true),
		Entry("Grace GPU with sysfs prefixes", "0x10de", "0x2348", true),
		Entry("Grace GPU with mixed case", "10de", "2941", true),
		Entry("non-Grace NVIDIA GPU", "10DE", "2330", false),
		Entry("non-NVIDIA device", "1AF4", "2342", false),
	)
})
