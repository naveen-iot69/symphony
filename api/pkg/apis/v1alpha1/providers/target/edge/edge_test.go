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
					Name: "softdpacd-1",
					Type: "container",
					Metadata: map[string]string{
						"OwnerId":                   "4b0c5d10-fb9c-414b-a331-1859f778f1f4",
						"Uuid":                      "1c014c0b-9f8f-4251-b138-4fde88492a9b",
						"labels.originalAppName":    "SoftdpacD_1",
						"labels.pairUniqueId":       "16af79fa971b4a1bbce180999d178fd3",
						"labels.partnerDeviceId":    "142292d7-dd0c-4a11-888e-3ad880ed4ce0",
						"labels.partnerInterlinkIP": "10.10.1.65",
						"labels.partnerName":        "SoftdpacC_1",
						"labels.partnerOwnerId":     "4b0c5d10-fb9c-414b-a331-1859f778f1f4",
						"labels.partnerSoftdpacIP":  "192.168.200.63",
						"labels.preferredPrimary":   "false",
						"labels.type":               "softdpac",
						"name":                      "softdpacd-1",
					},
					Properties: map[string]interface{}{
						"container": map[string]interface{}{
							"Image": "softdpac-ha:v25.0.25148.23",
							"Name":  "softdpacd-1",
							"Networks": []interface{}{
								map[string]interface{}{
									"Ipv4":      "192.168.200.65",
									"Ipv6":      "",
									"NetworkId": "softdpacDeviceNet",
								},
							},
							"Resources": map[string]interface{}{
								"Limits": map[string]interface{}{
									"Cpus":   "0,1,2,3",
									"Memory": "524288",
								},
							},
						},
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

	deploymentStep := createTestDeploymentStep()

	reference := model.TargetProviderApplyReference{
		TargetName: "39ac2cd3-a6a4-446e-94e2-074f4083a3d5",
		Step:       deploymentStep,
		IsDryRun:   true,
	}

	_, err = provider.Apply(context.Background(), reference)
	assert.Nil(t, err)
}

func TestApply(t *testing.T) {
	config := EdgeProviderConfig{
		Name: "test",
	}
	provider := EdgeProvider{}
	err := provider.Init(config)
	assert.Nil(t, err)

	deploymentStep := createTestDeploymentStep()

	reference := model.TargetProviderApplyReference{
		TargetName: "7613e804-a59d-4efc-a1d1-ada86954e3e0",
		Step:       deploymentStep,
		IsDryRun:   false,
	}

	componentResultSpec, err := provider.Apply(context.Background(), reference)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(componentResultSpec))
}
