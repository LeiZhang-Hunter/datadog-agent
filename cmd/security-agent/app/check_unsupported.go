// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build windows || !kubeapiserver

package app

import (
	"github.com/spf13/cobra"

	"github.com/DataDog/datadog-agent/cmd/security-agent/app/common"
)

func CheckCommands(globalParams *common.GlobalParams) []*cobra.Command {
	return nil
}
