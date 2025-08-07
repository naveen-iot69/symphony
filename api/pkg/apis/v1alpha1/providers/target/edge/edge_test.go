package edge

import (
	"context"
	"testing"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/stretchr/testify/assert"
)

func TestEdgeTargetProviderConfigFromMapNil(t *testing.T) {
	_, err := EdgeProviderConfigFromMap(nil)
	assert.NotNil(t, err)
}

func TestGet(t *testing.T) {
	config := EdgeProviderConfig{
		Name: "test",
	}
	provider := EdgeProvider{}
	err := provider.Init(config)
	assert.Nil(t, err)
	meta := model.ObjectMeta{
		UID: "142292d7-dd0c-4a11-888e-3ad880ed4ce0",
	}

	components, err := provider.Get(context.Background(), model.DeploymentSpec{
		Instance: model.InstanceState{
			Spec:       &model.InstanceSpec{},
			ObjectMeta: meta,
		},
		Solution: model.SolutionState{
			Spec: &model.SolutionSpec{
				Components: []model.ComponentSpec{
					{
						Name: "redis-test",
						Type: "container",
						Properties: map[string]interface{}{
							model.ContainerImage: "redis:latest",
						},
					},
				},
			},
		},
	}, []model.ComponentStep{
		{
			Action: model.ComponentUpdate,
			Component: model.ComponentSpec{
				Name: "redis-test",
				Type: "container",
				Metadata: map[string]string{
					"Uuid": "142292d7-dd0c-4a11-888e-3ad880ed4ce0",
				},
				Properties: map[string]interface{}{
					model.ContainerImage: "redis:latest",
					"env.REDIS_VERSION":  "7.0.12",
				},
			},
		},
	})
	assert.Equal(t, 1, len(components))
}

func createTestDeploymentSpec() model.DeploymentSpec {
	meta := model.ObjectMeta{
		UID: "d3971152-d47e-4956-8f7d-9b55a24a625c",
	}

	return model.DeploymentSpec{
		Instance: model.InstanceState{
			Spec:       &model.InstanceSpec{},
			ObjectMeta: meta,
		},
		Solution: model.SolutionState{
			Spec: &model.SolutionSpec{
				Components: []model.ComponentSpec{
					{
						Name: "redis-test",
						Type: "container",
						Properties: map[string]interface{}{
							model.ContainerImage: "redis:latest",
						},
					},
				},
			},
		},
	}
}

func createTestDeploymentStep() model.DeploymentStep {
	return model.DeploymentStep{
		Target: "d3971152-d47e-4956-8f7d-9b55a24a625c",
		Components: []model.ComponentStep{
			{
				Action: model.ComponentUpdate,
				Component: model.ComponentSpec{
					Name: "redis-test",
					Type: "container",
					Metadata: map[string]string{
						"Uuid": "142292d7-dd0c-4a11-888e-3ad880ed4ce0",
					},
					Properties: map[string]interface{}{
						model.ContainerImage: "redis:latest",
						"env.REDIS_VERSION":  "7.0.12",
						"app.id":             "142292d7-dd0c-4a11-888e-3ad880ed4ce0",
						"app.version":        "1.0.0",
					},
				},
			},
		},
	}
}

func TestApplyDryRun(t *testing.T) {
	config := EdgeProviderConfig{
		Name: "test",
	}
	provider := EdgeProvider{}
	err := provider.Init(config)
	assert.Nil(t, err)

	deploymentSpec := createTestDeploymentSpec()
	deploymentStep := createTestDeploymentStep()

	_, err = provider.Apply(context.Background(), deploymentSpec, deploymentStep, true)
	assert.Nil(t, err)
}

func TestApply(t *testing.T) {
	config := EdgeProviderConfig{
		Name: "test",
	}
	provider := EdgeProvider{}
	err := provider.Init(config)
	assert.Nil(t, err)

	deploymentSpec := createTestDeploymentSpec()
	deploymentStep := createTestDeploymentStep()

	componentResultSpec, err := provider.Apply(context.Background(), deploymentSpec, deploymentStep, false)
	assert.Equal(t, 1, len(componentResultSpec))
}
