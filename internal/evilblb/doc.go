// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

// Package evilblb provides a way to inject evils to a running Blb cluster
// to test its robustness. The package has defined several evil types and
// users can configure what types of evils that will be injected.
//
// EvilBlb will randomly pick a configured evil type and inject to the Blb
// cluster. After waiting for a configured amount of time EvilBlb will bring
// the cluster back to a sane state for next round of evil injection.
//
// For now EvilBlb supports following evil types:
//
// 1. PartitionMasterLeader: Partition master leader from the rest of
//    master replicas.
//
// 2. PartitionCuratorLeader: Partition curator leader from the rest of
//    the replicas of the group.
//
// 3. CorruptTracts: Corrupt some random tracts on a tract server.
//
// 4. KillCuratorLeader: Kill(restart) curator leader of randomly picked group.
//
// 5. KillMasterLeader: Kill(restart) master leader.
//
// 6. MultiEvil: A combination of evils above as one evil type.
//
// The following is an example of configuration for EvilBlb in JSON format.
// It has defined 5 evils types that will be brought to Blb cluster one at a
// time and a gap between [2m, 10m] will be introduced between evils.
//
/*
{
  "Params": {
    "User": "root",
    "Masters": ["aaaa18:4000", "aaaa19:4000", "aaaa20:4000"],
    "MinGapBetweenEvils": "2m",
    "MaxGapBetweenEvils": "10m",
    "TotalLength": "2h"
  },

  "Evils": [
    {
      "_comment": "Partition master leader for 1 min",
      "Name": "PartitionMasterLeader",
      "Length": "1m"
    },
    {
      "_comment": "Corrupt large amount of tracts",
      "Name": "CorruptTracts",
      "NumMin": 100,
      "NumMax": 10000
    },
    {
      "_comment": "Partition curator leader for 30s",
      "Name": "PartitionCuratorLeader",
      "Length" : "30s"
    },
    {
      "_comment": "Corrupt small amount of tracts",
      "Name": "CorruptTracts",
      "NumMin": 1,
      "NumMax": 20
    },
    {
      "_comment" : "A combination of tracts corruption and killing disk IO",
      "Name": "MultiEvil",
      "Evils": [
        {
          "_comment": "Corrupt large amount of tracts",
          "Name": "CorruptTracts",
          "NumMin": 100,
          "NumMax": 10000
        },
        {
          "_comment": "Burning disk IO",
          "Name": "BurnTractserverIO",
          "Length": "1m"
        }
      ]
    }
  ]
 }
*/

package evilblb
