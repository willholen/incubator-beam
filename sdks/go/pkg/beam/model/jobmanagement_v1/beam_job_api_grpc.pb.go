//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package jobmanagement_v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// JobServiceClient is the client API for JobService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type JobServiceClient interface {
	// Prepare a job for execution. The job will not be executed until a call is made to run with the
	// returned preparationId.
	Prepare(ctx context.Context, in *PrepareJobRequest, opts ...grpc.CallOption) (*PrepareJobResponse, error)
	// Submit the job for execution
	Run(ctx context.Context, in *RunJobRequest, opts ...grpc.CallOption) (*RunJobResponse, error)
	// Get a list of all invoked jobs
	GetJobs(ctx context.Context, in *GetJobsRequest, opts ...grpc.CallOption) (*GetJobsResponse, error)
	// Get the current state of the job
	GetState(ctx context.Context, in *GetJobStateRequest, opts ...grpc.CallOption) (*JobStateEvent, error)
	// Get the job's pipeline
	GetPipeline(ctx context.Context, in *GetJobPipelineRequest, opts ...grpc.CallOption) (*GetJobPipelineResponse, error)
	// Cancel the job
	Cancel(ctx context.Context, in *CancelJobRequest, opts ...grpc.CallOption) (*CancelJobResponse, error)
	// Subscribe to a stream of state changes of the job, will immediately return the current state of the job as the first response.
	GetStateStream(ctx context.Context, in *GetJobStateRequest, opts ...grpc.CallOption) (JobService_GetStateStreamClient, error)
	// Subscribe to a stream of state changes and messages from the job
	GetMessageStream(ctx context.Context, in *JobMessagesRequest, opts ...grpc.CallOption) (JobService_GetMessageStreamClient, error)
	// Fetch metrics for a given job
	GetJobMetrics(ctx context.Context, in *GetJobMetricsRequest, opts ...grpc.CallOption) (*GetJobMetricsResponse, error)
	// Get the supported pipeline options of the runner
	DescribePipelineOptions(ctx context.Context, in *DescribePipelineOptionsRequest, opts ...grpc.CallOption) (*DescribePipelineOptionsResponse, error)
}

type jobServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewJobServiceClient(cc grpc.ClientConnInterface) JobServiceClient {
	return &jobServiceClient{cc}
}

func (c *jobServiceClient) Prepare(ctx context.Context, in *PrepareJobRequest, opts ...grpc.CallOption) (*PrepareJobResponse, error) {
	out := new(PrepareJobResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/Prepare", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) Run(ctx context.Context, in *RunJobRequest, opts ...grpc.CallOption) (*RunJobResponse, error) {
	out := new(RunJobResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/Run", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) GetJobs(ctx context.Context, in *GetJobsRequest, opts ...grpc.CallOption) (*GetJobsResponse, error) {
	out := new(GetJobsResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/GetJobs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) GetState(ctx context.Context, in *GetJobStateRequest, opts ...grpc.CallOption) (*JobStateEvent, error) {
	out := new(JobStateEvent)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/GetState", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) GetPipeline(ctx context.Context, in *GetJobPipelineRequest, opts ...grpc.CallOption) (*GetJobPipelineResponse, error) {
	out := new(GetJobPipelineResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/GetPipeline", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) Cancel(ctx context.Context, in *CancelJobRequest, opts ...grpc.CallOption) (*CancelJobResponse, error) {
	out := new(CancelJobResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/Cancel", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) GetStateStream(ctx context.Context, in *GetJobStateRequest, opts ...grpc.CallOption) (JobService_GetStateStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &JobService_ServiceDesc.Streams[0], "/org.apache.beam.model.job_management.v1.JobService/GetStateStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobServiceGetStateStreamClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type JobService_GetStateStreamClient interface {
	Recv() (*JobStateEvent, error)
	grpc.ClientStream
}

type jobServiceGetStateStreamClient struct {
	grpc.ClientStream
}

func (x *jobServiceGetStateStreamClient) Recv() (*JobStateEvent, error) {
	m := new(JobStateEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *jobServiceClient) GetMessageStream(ctx context.Context, in *JobMessagesRequest, opts ...grpc.CallOption) (JobService_GetMessageStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &JobService_ServiceDesc.Streams[1], "/org.apache.beam.model.job_management.v1.JobService/GetMessageStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobServiceGetMessageStreamClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type JobService_GetMessageStreamClient interface {
	Recv() (*JobMessagesResponse, error)
	grpc.ClientStream
}

type jobServiceGetMessageStreamClient struct {
	grpc.ClientStream
}

func (x *jobServiceGetMessageStreamClient) Recv() (*JobMessagesResponse, error) {
	m := new(JobMessagesResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *jobServiceClient) GetJobMetrics(ctx context.Context, in *GetJobMetricsRequest, opts ...grpc.CallOption) (*GetJobMetricsResponse, error) {
	out := new(GetJobMetricsResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/GetJobMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) DescribePipelineOptions(ctx context.Context, in *DescribePipelineOptionsRequest, opts ...grpc.CallOption) (*DescribePipelineOptionsResponse, error) {
	out := new(DescribePipelineOptionsResponse)
	err := c.cc.Invoke(ctx, "/org.apache.beam.model.job_management.v1.JobService/DescribePipelineOptions", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobServiceServer is the server API for JobService service.
// All implementations must embed UnimplementedJobServiceServer
// for forward compatibility
type JobServiceServer interface {
	// Prepare a job for execution. The job will not be executed until a call is made to run with the
	// returned preparationId.
	Prepare(context.Context, *PrepareJobRequest) (*PrepareJobResponse, error)
	// Submit the job for execution
	Run(context.Context, *RunJobRequest) (*RunJobResponse, error)
	// Get a list of all invoked jobs
	GetJobs(context.Context, *GetJobsRequest) (*GetJobsResponse, error)
	// Get the current state of the job
	GetState(context.Context, *GetJobStateRequest) (*JobStateEvent, error)
	// Get the job's pipeline
	GetPipeline(context.Context, *GetJobPipelineRequest) (*GetJobPipelineResponse, error)
	// Cancel the job
	Cancel(context.Context, *CancelJobRequest) (*CancelJobResponse, error)
	// Subscribe to a stream of state changes of the job, will immediately return the current state of the job as the first response.
	GetStateStream(*GetJobStateRequest, JobService_GetStateStreamServer) error
	// Subscribe to a stream of state changes and messages from the job
	GetMessageStream(*JobMessagesRequest, JobService_GetMessageStreamServer) error
	// Fetch metrics for a given job
	GetJobMetrics(context.Context, *GetJobMetricsRequest) (*GetJobMetricsResponse, error)
	// Get the supported pipeline options of the runner
	DescribePipelineOptions(context.Context, *DescribePipelineOptionsRequest) (*DescribePipelineOptionsResponse, error)
	mustEmbedUnimplementedJobServiceServer()
}

// UnimplementedJobServiceServer must be embedded to have forward compatible implementations.
type UnimplementedJobServiceServer struct {
}

func (UnimplementedJobServiceServer) Prepare(context.Context, *PrepareJobRequest) (*PrepareJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Prepare not implemented")
}
func (UnimplementedJobServiceServer) Run(context.Context, *RunJobRequest) (*RunJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Run not implemented")
}
func (UnimplementedJobServiceServer) GetJobs(context.Context, *GetJobsRequest) (*GetJobsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobs not implemented")
}
func (UnimplementedJobServiceServer) GetState(context.Context, *GetJobStateRequest) (*JobStateEvent, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetState not implemented")
}
func (UnimplementedJobServiceServer) GetPipeline(context.Context, *GetJobPipelineRequest) (*GetJobPipelineResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPipeline not implemented")
}
func (UnimplementedJobServiceServer) Cancel(context.Context, *CancelJobRequest) (*CancelJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Cancel not implemented")
}
func (UnimplementedJobServiceServer) GetStateStream(*GetJobStateRequest, JobService_GetStateStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method GetStateStream not implemented")
}
func (UnimplementedJobServiceServer) GetMessageStream(*JobMessagesRequest, JobService_GetMessageStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method GetMessageStream not implemented")
}
func (UnimplementedJobServiceServer) GetJobMetrics(context.Context, *GetJobMetricsRequest) (*GetJobMetricsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobMetrics not implemented")
}
func (UnimplementedJobServiceServer) DescribePipelineOptions(context.Context, *DescribePipelineOptionsRequest) (*DescribePipelineOptionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DescribePipelineOptions not implemented")
}
func (UnimplementedJobServiceServer) mustEmbedUnimplementedJobServiceServer() {}

// UnsafeJobServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to JobServiceServer will
// result in compilation errors.
type UnsafeJobServiceServer interface {
	mustEmbedUnimplementedJobServiceServer()
}

func RegisterJobServiceServer(s grpc.ServiceRegistrar, srv JobServiceServer) {
	s.RegisterService(&JobService_ServiceDesc, srv)
}

func _JobService_Prepare_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PrepareJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).Prepare(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/Prepare",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).Prepare(ctx, req.(*PrepareJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_Run_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RunJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).Run(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/Run",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).Run(ctx, req.(*RunJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_GetJobs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetJobs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/GetJobs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetJobs(ctx, req.(*GetJobsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_GetState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobStateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/GetState",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetState(ctx, req.(*GetJobStateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_GetPipeline_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobPipelineRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetPipeline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/GetPipeline",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetPipeline(ctx, req.(*GetJobPipelineRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_Cancel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).Cancel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/Cancel",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).Cancel(ctx, req.(*CancelJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_GetStateStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetJobStateRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(JobServiceServer).GetStateStream(m, &jobServiceGetStateStreamServer{stream})
}

type JobService_GetStateStreamServer interface {
	Send(*JobStateEvent) error
	grpc.ServerStream
}

type jobServiceGetStateStreamServer struct {
	grpc.ServerStream
}

func (x *jobServiceGetStateStreamServer) Send(m *JobStateEvent) error {
	return x.ServerStream.SendMsg(m)
}

func _JobService_GetMessageStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(JobMessagesRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(JobServiceServer).GetMessageStream(m, &jobServiceGetMessageStreamServer{stream})
}

type JobService_GetMessageStreamServer interface {
	Send(*JobMessagesResponse) error
	grpc.ServerStream
}

type jobServiceGetMessageStreamServer struct {
	grpc.ServerStream
}

func (x *jobServiceGetMessageStreamServer) Send(m *JobMessagesResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _JobService_GetJobMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetJobMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/GetJobMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetJobMetrics(ctx, req.(*GetJobMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_DescribePipelineOptions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DescribePipelineOptionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).DescribePipelineOptions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/org.apache.beam.model.job_management.v1.JobService/DescribePipelineOptions",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).DescribePipelineOptions(ctx, req.(*DescribePipelineOptionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// JobService_ServiceDesc is the grpc.ServiceDesc for JobService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var JobService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "org.apache.beam.model.job_management.v1.JobService",
	HandlerType: (*JobServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Prepare",
			Handler:    _JobService_Prepare_Handler,
		},
		{
			MethodName: "Run",
			Handler:    _JobService_Run_Handler,
		},
		{
			MethodName: "GetJobs",
			Handler:    _JobService_GetJobs_Handler,
		},
		{
			MethodName: "GetState",
			Handler:    _JobService_GetState_Handler,
		},
		{
			MethodName: "GetPipeline",
			Handler:    _JobService_GetPipeline_Handler,
		},
		{
			MethodName: "Cancel",
			Handler:    _JobService_Cancel_Handler,
		},
		{
			MethodName: "GetJobMetrics",
			Handler:    _JobService_GetJobMetrics_Handler,
		},
		{
			MethodName: "DescribePipelineOptions",
			Handler:    _JobService_DescribePipelineOptions_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "GetStateStream",
			Handler:       _JobService_GetStateStream_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "GetMessageStream",
			Handler:       _JobService_GetMessageStream_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "beam_job_api.proto",
}
