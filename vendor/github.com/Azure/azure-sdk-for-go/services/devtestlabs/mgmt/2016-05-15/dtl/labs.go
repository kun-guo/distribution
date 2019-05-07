package dtl

// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"context"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/validation"
	"net/http"
)

// LabsClient is the the DevTest Labs Client.
type LabsClient struct {
	BaseClient
}

// NewLabsClient creates an instance of the LabsClient client.
func NewLabsClient(subscriptionID string) LabsClient {
	return NewLabsClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewLabsClientWithBaseURI creates an instance of the LabsClient client.
func NewLabsClientWithBaseURI(baseURI string, subscriptionID string) LabsClient {
	return LabsClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// ClaimAnyVM claim a random claimable virtual machine in the lab. This operation can take a while to complete.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
func (client LabsClient) ClaimAnyVM(ctx context.Context, resourceGroupName string, name string) (result LabsClaimAnyVMFuture, err error) {
	req, err := client.ClaimAnyVMPreparer(ctx, resourceGroupName, name)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ClaimAnyVM", nil, "Failure preparing request")
		return
	}

	result, err = client.ClaimAnyVMSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ClaimAnyVM", result.Response(), "Failure sending request")
		return
	}

	return
}

// ClaimAnyVMPreparer prepares the ClaimAnyVM request.
func (client LabsClient) ClaimAnyVMPreparer(ctx context.Context, resourceGroupName string, name string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}/claimAnyVm", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ClaimAnyVMSender sends the ClaimAnyVM request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) ClaimAnyVMSender(req *http.Request) (future LabsClaimAnyVMFuture, err error) {
	sender := autorest.DecorateSender(client, azure.DoRetryWithRegistration(client.Client))
	future.Future = azure.NewFuture(req)
	future.req = req
	_, err = future.Done(sender)
	if err != nil {
		return
	}
	err = autorest.Respond(future.Response(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted))
	return
}

// ClaimAnyVMResponder handles the response to the ClaimAnyVM request. The method always
// closes the http.Response Body.
func (client LabsClient) ClaimAnyVMResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

// CreateEnvironment create virtual machines in a lab. This operation can take a while to complete.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
// labVirtualMachineCreationParameter - properties for creating a virtual machine.
func (client LabsClient) CreateEnvironment(ctx context.Context, resourceGroupName string, name string, labVirtualMachineCreationParameter LabVirtualMachineCreationParameter) (result LabsCreateEnvironmentFuture, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: labVirtualMachineCreationParameter,
			Constraints: []validation.Constraint{{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties", Name: validation.Null, Rule: false,
				Chain: []validation.Constraint{{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties.ApplicableSchedule", Name: validation.Null, Rule: false,
					Chain: []validation.Constraint{{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties.ApplicableSchedule.ApplicableScheduleProperties", Name: validation.Null, Rule: true,
						Chain: []validation.Constraint{{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties.ApplicableSchedule.ApplicableScheduleProperties.LabVmsShutdown", Name: validation.Null, Rule: false,
							Chain: []validation.Constraint{{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties.ApplicableSchedule.ApplicableScheduleProperties.LabVmsShutdown.ScheduleProperties", Name: validation.Null, Rule: true, Chain: nil}}},
							{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties.ApplicableSchedule.ApplicableScheduleProperties.LabVmsStartup", Name: validation.Null, Rule: false,
								Chain: []validation.Constraint{{Target: "labVirtualMachineCreationParameter.LabVirtualMachineCreationParameterProperties.ApplicableSchedule.ApplicableScheduleProperties.LabVmsStartup.ScheduleProperties", Name: validation.Null, Rule: true, Chain: nil}}},
						}},
					}},
				}}}}}); err != nil {
		return result, validation.NewError("dtl.LabsClient", "CreateEnvironment", err.Error())
	}

	req, err := client.CreateEnvironmentPreparer(ctx, resourceGroupName, name, labVirtualMachineCreationParameter)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "CreateEnvironment", nil, "Failure preparing request")
		return
	}

	result, err = client.CreateEnvironmentSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "CreateEnvironment", result.Response(), "Failure sending request")
		return
	}

	return
}

// CreateEnvironmentPreparer prepares the CreateEnvironment request.
func (client LabsClient) CreateEnvironmentPreparer(ctx context.Context, resourceGroupName string, name string, labVirtualMachineCreationParameter LabVirtualMachineCreationParameter) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}/createEnvironment", pathParameters),
		autorest.WithJSON(labVirtualMachineCreationParameter),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// CreateEnvironmentSender sends the CreateEnvironment request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) CreateEnvironmentSender(req *http.Request) (future LabsCreateEnvironmentFuture, err error) {
	sender := autorest.DecorateSender(client, azure.DoRetryWithRegistration(client.Client))
	future.Future = azure.NewFuture(req)
	future.req = req
	_, err = future.Done(sender)
	if err != nil {
		return
	}
	err = autorest.Respond(future.Response(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted))
	return
}

// CreateEnvironmentResponder handles the response to the CreateEnvironment request. The method always
// closes the http.Response Body.
func (client LabsClient) CreateEnvironmentResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

// CreateOrUpdate create or replace an existing lab. This operation can take a while to complete.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
// lab - a lab.
func (client LabsClient) CreateOrUpdate(ctx context.Context, resourceGroupName string, name string, lab Lab) (result LabsCreateOrUpdateFuture, err error) {
	req, err := client.CreateOrUpdatePreparer(ctx, resourceGroupName, name, lab)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "CreateOrUpdate", nil, "Failure preparing request")
		return
	}

	result, err = client.CreateOrUpdateSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "CreateOrUpdate", result.Response(), "Failure sending request")
		return
	}

	return
}

// CreateOrUpdatePreparer prepares the CreateOrUpdate request.
func (client LabsClient) CreateOrUpdatePreparer(ctx context.Context, resourceGroupName string, name string, lab Lab) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPut(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}", pathParameters),
		autorest.WithJSON(lab),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// CreateOrUpdateSender sends the CreateOrUpdate request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) CreateOrUpdateSender(req *http.Request) (future LabsCreateOrUpdateFuture, err error) {
	sender := autorest.DecorateSender(client, azure.DoRetryWithRegistration(client.Client))
	future.Future = azure.NewFuture(req)
	future.req = req
	_, err = future.Done(sender)
	if err != nil {
		return
	}
	err = autorest.Respond(future.Response(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusCreated))
	return
}

// CreateOrUpdateResponder handles the response to the CreateOrUpdate request. The method always
// closes the http.Response Body.
func (client LabsClient) CreateOrUpdateResponder(resp *http.Response) (result Lab, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusCreated),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// Delete delete lab. This operation can take a while to complete.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
func (client LabsClient) Delete(ctx context.Context, resourceGroupName string, name string) (result LabsDeleteFuture, err error) {
	req, err := client.DeletePreparer(ctx, resourceGroupName, name)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Delete", nil, "Failure preparing request")
		return
	}

	result, err = client.DeleteSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Delete", result.Response(), "Failure sending request")
		return
	}

	return
}

// DeletePreparer prepares the Delete request.
func (client LabsClient) DeletePreparer(ctx context.Context, resourceGroupName string, name string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsDelete(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// DeleteSender sends the Delete request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) DeleteSender(req *http.Request) (future LabsDeleteFuture, err error) {
	sender := autorest.DecorateSender(client, azure.DoRetryWithRegistration(client.Client))
	future.Future = azure.NewFuture(req)
	future.req = req
	_, err = future.Done(sender)
	if err != nil {
		return
	}
	err = autorest.Respond(future.Response(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted, http.StatusNoContent))
	return
}

// DeleteResponder handles the response to the Delete request. The method always
// closes the http.Response Body.
func (client LabsClient) DeleteResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted, http.StatusNoContent),
		autorest.ByClosing())
	result.Response = resp
	return
}

// ExportResourceUsage exports the lab resource usage into a storage account This operation can take a while to
// complete.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
// exportResourceUsageParameters - the parameters of the export operation.
func (client LabsClient) ExportResourceUsage(ctx context.Context, resourceGroupName string, name string, exportResourceUsageParameters ExportResourceUsageParameters) (result LabsExportResourceUsageFuture, err error) {
	req, err := client.ExportResourceUsagePreparer(ctx, resourceGroupName, name, exportResourceUsageParameters)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ExportResourceUsage", nil, "Failure preparing request")
		return
	}

	result, err = client.ExportResourceUsageSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ExportResourceUsage", result.Response(), "Failure sending request")
		return
	}

	return
}

// ExportResourceUsagePreparer prepares the ExportResourceUsage request.
func (client LabsClient) ExportResourceUsagePreparer(ctx context.Context, resourceGroupName string, name string, exportResourceUsageParameters ExportResourceUsageParameters) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}/exportResourceUsage", pathParameters),
		autorest.WithJSON(exportResourceUsageParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ExportResourceUsageSender sends the ExportResourceUsage request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) ExportResourceUsageSender(req *http.Request) (future LabsExportResourceUsageFuture, err error) {
	sender := autorest.DecorateSender(client, azure.DoRetryWithRegistration(client.Client))
	future.Future = azure.NewFuture(req)
	future.req = req
	_, err = future.Done(sender)
	if err != nil {
		return
	}
	err = autorest.Respond(future.Response(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted))
	return
}

// ExportResourceUsageResponder handles the response to the ExportResourceUsage request. The method always
// closes the http.Response Body.
func (client LabsClient) ExportResourceUsageResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

// GenerateUploadURI generate a URI for uploading custom disk images to a Lab.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
// generateUploadURIParameter - properties for generating an upload URI.
func (client LabsClient) GenerateUploadURI(ctx context.Context, resourceGroupName string, name string, generateUploadURIParameter GenerateUploadURIParameter) (result GenerateUploadURIResponse, err error) {
	req, err := client.GenerateUploadURIPreparer(ctx, resourceGroupName, name, generateUploadURIParameter)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "GenerateUploadURI", nil, "Failure preparing request")
		return
	}

	resp, err := client.GenerateUploadURISender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "GenerateUploadURI", resp, "Failure sending request")
		return
	}

	result, err = client.GenerateUploadURIResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "GenerateUploadURI", resp, "Failure responding to request")
	}

	return
}

// GenerateUploadURIPreparer prepares the GenerateUploadURI request.
func (client LabsClient) GenerateUploadURIPreparer(ctx context.Context, resourceGroupName string, name string, generateUploadURIParameter GenerateUploadURIParameter) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}/generateUploadUri", pathParameters),
		autorest.WithJSON(generateUploadURIParameter),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// GenerateUploadURISender sends the GenerateUploadURI request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) GenerateUploadURISender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// GenerateUploadURIResponder handles the response to the GenerateUploadURI request. The method always
// closes the http.Response Body.
func (client LabsClient) GenerateUploadURIResponder(resp *http.Response) (result GenerateUploadURIResponse, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// Get get lab.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
// expand - specify the $expand query. Example: 'properties($select=defaultStorageAccount)'
func (client LabsClient) Get(ctx context.Context, resourceGroupName string, name string, expand string) (result Lab, err error) {
	req, err := client.GetPreparer(ctx, resourceGroupName, name, expand)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Get", nil, "Failure preparing request")
		return
	}

	resp, err := client.GetSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Get", resp, "Failure sending request")
		return
	}

	result, err = client.GetResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Get", resp, "Failure responding to request")
	}

	return
}

// GetPreparer prepares the Get request.
func (client LabsClient) GetPreparer(ctx context.Context, resourceGroupName string, name string, expand string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}
	if len(expand) > 0 {
		queryParameters["$expand"] = autorest.Encode("query", expand)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// GetSender sends the Get request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) GetSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// GetResponder handles the response to the Get request. The method always
// closes the http.Response Body.
func (client LabsClient) GetResponder(resp *http.Response) (result Lab, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListByResourceGroup list labs in a resource group.
// Parameters:
// resourceGroupName - the name of the resource group.
// expand - specify the $expand query. Example: 'properties($select=defaultStorageAccount)'
// filter - the filter to apply to the operation.
// top - the maximum number of resources to return from the operation.
// orderby - the ordering expression for the results, using OData notation.
func (client LabsClient) ListByResourceGroup(ctx context.Context, resourceGroupName string, expand string, filter string, top *int32, orderby string) (result ResponseWithContinuationLabPage, err error) {
	result.fn = client.listByResourceGroupNextResults
	req, err := client.ListByResourceGroupPreparer(ctx, resourceGroupName, expand, filter, top, orderby)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListByResourceGroup", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListByResourceGroupSender(req)
	if err != nil {
		result.rwcl.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListByResourceGroup", resp, "Failure sending request")
		return
	}

	result.rwcl, err = client.ListByResourceGroupResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListByResourceGroup", resp, "Failure responding to request")
	}

	return
}

// ListByResourceGroupPreparer prepares the ListByResourceGroup request.
func (client LabsClient) ListByResourceGroupPreparer(ctx context.Context, resourceGroupName string, expand string, filter string, top *int32, orderby string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}
	if len(expand) > 0 {
		queryParameters["$expand"] = autorest.Encode("query", expand)
	}
	if len(filter) > 0 {
		queryParameters["$filter"] = autorest.Encode("query", filter)
	}
	if top != nil {
		queryParameters["$top"] = autorest.Encode("query", *top)
	}
	if len(orderby) > 0 {
		queryParameters["$orderby"] = autorest.Encode("query", orderby)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListByResourceGroupSender sends the ListByResourceGroup request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) ListByResourceGroupSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// ListByResourceGroupResponder handles the response to the ListByResourceGroup request. The method always
// closes the http.Response Body.
func (client LabsClient) ListByResourceGroupResponder(resp *http.Response) (result ResponseWithContinuationLab, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// listByResourceGroupNextResults retrieves the next set of results, if any.
func (client LabsClient) listByResourceGroupNextResults(lastResults ResponseWithContinuationLab) (result ResponseWithContinuationLab, err error) {
	req, err := lastResults.responseWithContinuationLabPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "dtl.LabsClient", "listByResourceGroupNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}
	resp, err := client.ListByResourceGroupSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "dtl.LabsClient", "listByResourceGroupNextResults", resp, "Failure sending next results request")
	}
	result, err = client.ListByResourceGroupResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "listByResourceGroupNextResults", resp, "Failure responding to next results request")
	}
	return
}

// ListByResourceGroupComplete enumerates all values, automatically crossing page boundaries as required.
func (client LabsClient) ListByResourceGroupComplete(ctx context.Context, resourceGroupName string, expand string, filter string, top *int32, orderby string) (result ResponseWithContinuationLabIterator, err error) {
	result.page, err = client.ListByResourceGroup(ctx, resourceGroupName, expand, filter, top, orderby)
	return
}

// ListBySubscription list labs in a subscription.
// Parameters:
// expand - specify the $expand query. Example: 'properties($select=defaultStorageAccount)'
// filter - the filter to apply to the operation.
// top - the maximum number of resources to return from the operation.
// orderby - the ordering expression for the results, using OData notation.
func (client LabsClient) ListBySubscription(ctx context.Context, expand string, filter string, top *int32, orderby string) (result ResponseWithContinuationLabPage, err error) {
	result.fn = client.listBySubscriptionNextResults
	req, err := client.ListBySubscriptionPreparer(ctx, expand, filter, top, orderby)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListBySubscription", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListBySubscriptionSender(req)
	if err != nil {
		result.rwcl.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListBySubscription", resp, "Failure sending request")
		return
	}

	result.rwcl, err = client.ListBySubscriptionResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListBySubscription", resp, "Failure responding to request")
	}

	return
}

// ListBySubscriptionPreparer prepares the ListBySubscription request.
func (client LabsClient) ListBySubscriptionPreparer(ctx context.Context, expand string, filter string, top *int32, orderby string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"subscriptionId": autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}
	if len(expand) > 0 {
		queryParameters["$expand"] = autorest.Encode("query", expand)
	}
	if len(filter) > 0 {
		queryParameters["$filter"] = autorest.Encode("query", filter)
	}
	if top != nil {
		queryParameters["$top"] = autorest.Encode("query", *top)
	}
	if len(orderby) > 0 {
		queryParameters["$orderby"] = autorest.Encode("query", orderby)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/providers/Microsoft.DevTestLab/labs", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListBySubscriptionSender sends the ListBySubscription request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) ListBySubscriptionSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// ListBySubscriptionResponder handles the response to the ListBySubscription request. The method always
// closes the http.Response Body.
func (client LabsClient) ListBySubscriptionResponder(resp *http.Response) (result ResponseWithContinuationLab, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// listBySubscriptionNextResults retrieves the next set of results, if any.
func (client LabsClient) listBySubscriptionNextResults(lastResults ResponseWithContinuationLab) (result ResponseWithContinuationLab, err error) {
	req, err := lastResults.responseWithContinuationLabPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "dtl.LabsClient", "listBySubscriptionNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}
	resp, err := client.ListBySubscriptionSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "dtl.LabsClient", "listBySubscriptionNextResults", resp, "Failure sending next results request")
	}
	result, err = client.ListBySubscriptionResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "listBySubscriptionNextResults", resp, "Failure responding to next results request")
	}
	return
}

// ListBySubscriptionComplete enumerates all values, automatically crossing page boundaries as required.
func (client LabsClient) ListBySubscriptionComplete(ctx context.Context, expand string, filter string, top *int32, orderby string) (result ResponseWithContinuationLabIterator, err error) {
	result.page, err = client.ListBySubscription(ctx, expand, filter, top, orderby)
	return
}

// ListVhds list disk images available for custom image creation.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
func (client LabsClient) ListVhds(ctx context.Context, resourceGroupName string, name string) (result ResponseWithContinuationLabVhdPage, err error) {
	result.fn = client.listVhdsNextResults
	req, err := client.ListVhdsPreparer(ctx, resourceGroupName, name)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListVhds", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListVhdsSender(req)
	if err != nil {
		result.rwclv.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListVhds", resp, "Failure sending request")
		return
	}

	result.rwclv, err = client.ListVhdsResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "ListVhds", resp, "Failure responding to request")
	}

	return
}

// ListVhdsPreparer prepares the ListVhds request.
func (client LabsClient) ListVhdsPreparer(ctx context.Context, resourceGroupName string, name string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}/listVhds", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListVhdsSender sends the ListVhds request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) ListVhdsSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// ListVhdsResponder handles the response to the ListVhds request. The method always
// closes the http.Response Body.
func (client LabsClient) ListVhdsResponder(resp *http.Response) (result ResponseWithContinuationLabVhd, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// listVhdsNextResults retrieves the next set of results, if any.
func (client LabsClient) listVhdsNextResults(lastResults ResponseWithContinuationLabVhd) (result ResponseWithContinuationLabVhd, err error) {
	req, err := lastResults.responseWithContinuationLabVhdPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "dtl.LabsClient", "listVhdsNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}
	resp, err := client.ListVhdsSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "dtl.LabsClient", "listVhdsNextResults", resp, "Failure sending next results request")
	}
	result, err = client.ListVhdsResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "listVhdsNextResults", resp, "Failure responding to next results request")
	}
	return
}

// ListVhdsComplete enumerates all values, automatically crossing page boundaries as required.
func (client LabsClient) ListVhdsComplete(ctx context.Context, resourceGroupName string, name string) (result ResponseWithContinuationLabVhdIterator, err error) {
	result.page, err = client.ListVhds(ctx, resourceGroupName, name)
	return
}

// Update modify properties of labs.
// Parameters:
// resourceGroupName - the name of the resource group.
// name - the name of the lab.
// lab - a lab.
func (client LabsClient) Update(ctx context.Context, resourceGroupName string, name string, lab LabFragment) (result Lab, err error) {
	req, err := client.UpdatePreparer(ctx, resourceGroupName, name, lab)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Update", nil, "Failure preparing request")
		return
	}

	resp, err := client.UpdateSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Update", resp, "Failure sending request")
		return
	}

	result, err = client.UpdateResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "dtl.LabsClient", "Update", resp, "Failure responding to request")
	}

	return
}

// UpdatePreparer prepares the Update request.
func (client LabsClient) UpdatePreparer(ctx context.Context, resourceGroupName string, name string, lab LabFragment) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"name":              autorest.Encode("path", name),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2016-05-15"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPatch(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DevTestLab/labs/{name}", pathParameters),
		autorest.WithJSON(lab),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// UpdateSender sends the Update request. The method will close the
// http.Response Body if it receives an error.
func (client LabsClient) UpdateSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// UpdateResponder handles the response to the Update request. The method always
// closes the http.Response Body.
func (client LabsClient) UpdateResponder(resp *http.Response) (result Lab, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}
