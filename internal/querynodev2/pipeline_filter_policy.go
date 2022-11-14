// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynodev2

import (
	"errors"
	"fmt"
)

type InsertMsgFilter = func(c *Collection, msg *InsertMsg) error
type DeleteMsgFilter = func(c *Collection, msg *DeleteMsg) error

func NotAlignedInsert(c *Collection, msg *InsertMsg) error {
	err := msg.CheckAligned()
	return fmt.Errorf("checkAligned failed, err = %s", err)
}

func EmptyInsert(c *Collection, msg *InsertMsg) error {
	if len(msg.GetTimestamps()) <= 0 {
		return errors.New("empty insert message")
	}
	return nil
}

func StrayedInsert(c *Collection, msg *InsertMsg) error {
	if msg.GetCollectionID() != c.ID() {
		return errors.New("not target collection")
	}

	if c.GetLoadType() == loadTypePartition {
		if c.HasPartition(msg.PartitionID) {
			return errors.New("not target partition")
		}
	}
	return nil
}

func NotAlignedDelete(c *Collection, msg *DeleteMsg) error {
	err := msg.CheckAligned()
	return fmt.Errorf("checkAligned failed, err = %s", err)
}

func EmptyDelete(c *Collection, msg *DeleteMsg) error {
	if len(msg.GetTimestamps()) <= 0 {
		return errors.New("empty Delete message")
	}
	return nil
}

func StrayedDelete(c *Collection, msg *DeleteMsg) error {
	if msg.GetCollectionID() != c.ID() {
		return errors.New("not target collection")
	}

	if c.GetLoadType() == loadTypePartition {
		if c.HasPartition(msg.PartitionID) {
			return errors.New("not target partition")
		}
	}
	return nil
}
