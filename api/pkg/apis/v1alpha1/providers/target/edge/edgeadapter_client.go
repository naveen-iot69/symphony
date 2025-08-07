// SPDX-FileCopyrightText: (C) 2024 Intel Corporation
//
// SPDX-License-Identifier: LicenseRef-Intel

package edge

import (
	"context"
	"crypto/tls"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/edge/api/edge_adapter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func NewEdgeAdapterClient(ctx context.Context, token string, tlsCredentials *tls.Config) (edge_adapter.EdgeAdapterGrpcClient, error) {
	addr := "eaep25:6201"

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCredentials)),
	}

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.Header(&metadata.MD{"content-type": []string{"application/grpc"}})))

	if token != "" {
		conn, err := grpc.DialContext(ctx, addr, opts...)
		if err != nil {
			return nil, err
		}
		return edge_adapter.NewEdgeAdapterGrpcClient(conn), nil
	} else {
		conn, err := grpc.DialContext(ctx, addr, opts...)
		if err != nil {
			return nil, err
		}
		return edge_adapter.NewEdgeAdapterGrpcClient(conn), nil
	}
}
