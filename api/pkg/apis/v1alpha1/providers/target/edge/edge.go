package edge

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/contexts"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	southbound "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/edge/api/edge_adapter"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/edge/api/system_model"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/edge/authprovider"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	observ_utils "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability/utils"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const loggerName = "providers.target.edge"

var sLog = logger.NewLogger(loggerName)

var (
	BaseAddress = "https://EAEP25:6201"
)

type EdgeProviderConfig struct {
	Name string `json:"name"`
}

type EdgeProvider struct {
	Context *contexts.ManagerContext
	Config  EdgeProviderConfig

	AuthService       *authprovider.AuthenticationService
	SystemClient      system_model.SystemModelClient
	EdgeAdapterClient southbound.EdgeAdapterServiceClient
}

func EdgeProviderConfigFromMap(properties map[string]string) (EdgeProviderConfig, error) {
	config := EdgeProviderConfig{}

	if name, ok := properties["name"]; ok {
		config.Name = name
	}
	return config, nil
}

func (h *EdgeProvider) InitWithMap(properties map[string]string) error {
	config, err := EdgeProviderConfigFromMap(properties)
	if err != nil {
		return err
	}
	return h.Init(config)
}

func (h *EdgeProvider) SetContext(ctx *contexts.ManagerContext) {
	h.Context = ctx
}

func (h *EdgeProvider) Init(config providers.IProviderConfig) error {
	ctx, span := observability.StartSpan("Edge Target Provider", context.TODO(), &map[string]string{
		"method": "Init",
	})
	var err error = nil
	defer observ_utils.CloseSpanWithError(span, &err)
	defer observ_utils.EmitUserDiagnosticsLogs(ctx, &err)

	sLog.InfoCtx(ctx, "  P (Edge Target): Init()")

	edgeConfig, err := toEdgeProviderConfig(config)
	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to convert provider config", "error", err)
		return err
	}

	h.Config = edgeConfig
	h.AuthService, err = authprovider.NewAuthenticationService()
	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to initialize authentication service", "error", err)
		return err
	}
	return nil
}

func toEdgeProviderConfig(config providers.IProviderConfig) (EdgeProviderConfig, error) {
	ret := EdgeProviderConfig{}
	data, err := json.Marshal(config)
	if err != nil {
		return ret, err
	}
	err = json.Unmarshal(data, &ret)
	return ret, err
}

func (h *EdgeProvider) connectToAPI(ctx context.Context, sessionId string, credentials *tls.Config, endpoint string) error {
	var err error
	h.SystemClient, err = NewSystemModelClient(ctx, sessionId, credentials)
	return err
}

func (h *EdgeProvider) establishConnection(ctx context.Context) (context.Context, error) {
	sessionID, _, err := h.AuthService.GetSessionIdAsync(BaseAddress)
	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to get session ID", "error", err)
		return ctx, err
	}

	md := metadata.Pairs(
		"cookie", fmt.Sprintf("sessionId=%s", sessionID),
		"content-type", "application/grpc",
	)
	ctxNew := metadata.NewOutgoingContext(ctx, md)

	if err := h.connectToAPI(ctxNew, sessionID, h.AuthService.Credentials, ""); err != nil {
		sLog.ErrorCtx(ctx, "Failed to connect to API", "error", err)
		return ctxNew, err
	}

	return ctxNew, nil
}

func (h *EdgeProvider) Get(ctx context.Context, deployment model.DeploymentSpec, references []model.ComponentStep) ([]model.ComponentSpec, error) {
	ctx, span := observability.StartSpan("Edge Target Provider", ctx, &map[string]string{
		"method": "Get",
	})
	var err error = nil
	defer observ_utils.CloseSpanWithError(span, &err)
	defer observ_utils.EmitUserDiagnosticsLogs(ctx, &err)

	sLog.InfoCtx(ctx, "  P (Edge Target): getting artifacts: %s - %s", deployment.Instance.Spec.Scope, deployment.Instance.ObjectMeta.Name)

	// ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	requestCtx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	// Session ID is taken every time Get() function is called
	requestCtxNew, err := h.establishConnection(requestCtx)
	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to establish connection", "error", err)
		return nil, err
	}

	app, err := h.SystemClient.GetAppInstanceById(requestCtxNew, wrapperspb.String(string(deployment.Instance.ObjectMeta.UID)))

	sLog.Info(app)

	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to get app by ID", "error", err)
		return nil, err
	}

	if app == nil {
		sLog.ErrorCtx(ctx, "app not found", "appName", deployment.Instance.ObjectMeta.Name)
		return nil, fmt.Errorf("app %s not found", deployment.Instance.ObjectMeta.Name)
	}

	compSpec := appToComponentSpec(app)

	return []model.ComponentSpec{compSpec}, nil
}

func appToComponentSpec(app *system_model.AppInstance) model.ComponentSpec {
	metadata := make(map[string]string)
	metadata["Uuid"] = app.Metadata.Uuid
	metadata["OnwerId"] = app.Metadata.OwnerId
	for k, v := range app.Metadata.Labels {
		metadata["labels."+k] = v
	}

	metadata["namespace"] = "default"
	metadata["etag"] = "1"

	compSpec := model.ComponentSpec{
		Name:     app.Metadata.Name,
		Type:     app.Kind,
		Metadata: metadata,
		// Properties: map[string]interface{}{
		// 	"container": device.Spec..(*system_model.AppInstanceSpec_Container).Container,
		// },
	}
	return compSpec
}

func (h *EdgeProvider) connectToEdgeAdapter(ctx context.Context, sessionId string, credentials *tls.Config) error {
	var err error
	h.EdgeAdapterClient, err = NewEdgeAdapterClient(ctx, sessionId, credentials)
	return err
}

func (h *EdgeProvider) establishEdgeConnection(ctx context.Context) (context.Context, error) {
	sessionID, _, err := h.AuthService.GetSessionIdAsync(BaseAddress)
	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to get session ID", "error", err)
		return ctx, err
	}

	md := metadata.Pairs(
		"cookie", fmt.Sprintf("sessionId=%s", sessionID),
		"content-type", "application/grpc",
	)
	ctxNew := metadata.NewOutgoingContext(ctx, md)

	if err := h.connectToAPI(ctxNew, sessionID, h.AuthService.Credentials, ""); err != nil {
		sLog.ErrorCtx(ctx, "Failed to connect to API", "error", err)
		return ctxNew, err
	}

	os.Setenv("EDGE_ADAPTER_SERVICE_ADDRESS", BaseAddress)

	if err := h.connectToEdgeAdapter(ctxNew, sessionID, h.AuthService.Credentials); err != nil {
		sLog.ErrorCtx(ctx, "Failed to connect to EdgeAdapter service", "error", err)
		return ctxNew, err
	}

	return ctxNew, nil
}

func (h *EdgeProvider) GetValidationRule(ctx context.Context) model.ValidationRule {
	return model.ValidationRule{
		AllowSidecar: false,
		ComponentValidationRule: model.ComponentValidationRule{
			RequiredProperties:    []string{"app.id", "app.version"},
			OptionalProperties:    []string{},
			RequiredComponentType: "",
			RequiredMetadata:      []string{},
			OptionalMetadata:      []string{},
			ChangeDetectionProperties: []model.PropertyDesc{
				{Name: "app.id", IgnoreCase: false, SkipIfMissing: false},
				{Name: "app.version", IgnoreCase: false, SkipIfMissing: false},
			},
		},
	}
}

func (h *EdgeProvider) Apply(ctx context.Context, deployment model.DeploymentSpec, step model.DeploymentStep, isDryRun bool) (map[string]model.ComponentResultSpec, error) {
	ctx, span := observability.StartSpan("Edge Target Provider", ctx, &map[string]string{
		"method": "Apply",
	})
	var err error = nil
	defer observ_utils.CloseSpanWithError(span, &err)
	defer observ_utils.EmitUserDiagnosticsLogs(ctx, &err)

	sLog.InfoCtx(ctx, "  P (Edge Target): Apply()")

	validationRule := h.GetValidationRule(ctx)
	components := make([]model.ComponentSpec, len(step.Components))
	for i, componentStep := range step.Components {
		components[i] = componentStep.Component
	}
	if validationErr := validationRule.Validate(components); validationErr != nil {
		sLog.ErrorCtx(ctx, "Component validation failed", "error", validationErr)
		return nil, validationErr
	}

	if isDryRun {
		sLog.InfoCtx(ctx, "Dry run mode - skipping actual deployment")
		return make(map[string]model.ComponentResultSpec), nil
	}

	requestCtx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFunc()

	requestCtxNew, err := h.establishEdgeConnection(requestCtx)
	if err != nil {
		sLog.ErrorCtx(ctx, "Failed to establish edge connection", "error", err)
		return nil, err
	}

	results := make(map[string]model.ComponentResultSpec)

	for _, componentStep := range step.Components {
		if componentStep.Action == model.ComponentUpdate {
			result, err := h.deployEdgeComponent(requestCtxNew, componentStep.Component)
			if err != nil {
				sLog.ErrorCtx(ctx, "Failed to deploy component", "component", componentStep.Component.Name, "error", err)
				results[componentStep.Component.Name] = model.ComponentResultSpec{
					Status:  v1alpha2.UpdateFailed,
					Message: fmt.Sprintf("Failed to deploy component: %v", err),
				}
			} else {
				results[componentStep.Component.Name] = result
			}
		}
	}

	return results, nil
}

func (h *EdgeProvider) deployEdgeComponent(ctx context.Context, component model.ComponentSpec) (model.ComponentResultSpec, error) {
	appID, ok := component.Properties["app.id"]
	if !ok {
		return model.ComponentResultSpec{}, fmt.Errorf("app.id is required")
	}
	appVersion, ok := component.Properties["app.version"]
	if !ok {
		return model.ComponentResultSpec{}, fmt.Errorf("app.version is required")
	}

	request := &southbound.EdgeAdapterGrpcRequest{
		Name:   component.Name,
		Kind:   component.Type,
		Labels: make(map[string]string),
		AppSpec: &southbound.EdgeAppSpec{
			Name:  fmt.Sprintf("%s", appID),
			Image: fmt.Sprintf("%s:%s", appID, appVersion),
		},
	}

	for key, value := range component.Properties {
		request.Labels[key] = fmt.Sprintf("%v", value)
	}
	if deviceID, exists := component.Properties["device.id"]; exists {
		request.Node = &southbound.Node{
			DeviceId: fmt.Sprintf("%v", deviceID),
		}
	} else {
		request.NodeSpec = &southbound.NodeSpec{
			Addresses: []string{"default"},
		}
	}

	if request.AppSpec.Resources == nil {
		request.AppSpec.Resources = &southbound.Resource{
			Limits: make(map[string]string),
		}
	}

	if memory, exists := component.Properties["resources.memory"]; exists {
		request.AppSpec.Resources.Limits["memory"] = fmt.Sprintf("%v", memory)
	}
	if cpu, exists := component.Properties["resources.cpu"]; exists {
		request.AppSpec.Resources.Limits["cpu"] = fmt.Sprintf("%v", cpu)
	}

	response, err := h.EdgeAdapterClient.DeployAsync(ctx, request)
	if err != nil {
		return model.ComponentResultSpec{}, fmt.Errorf("failed to deploy via EdgeAdapter: %w", err)
	}

	if response.HttpCode != 200 {
		return model.ComponentResultSpec{
			Status:  v1alpha2.UpdateFailed,
			Message: fmt.Sprintf("EdgeAdapter deployment failed: HTTP %d, Error %d, %s", response.HttpCode, response.ErrorCode, response.Message),
		}, nil
	}

	return model.ComponentResultSpec{
		Status:  v1alpha2.Updated,
		Message: "Component deployed successfully via EdgeAdapter",
	}, nil
}
