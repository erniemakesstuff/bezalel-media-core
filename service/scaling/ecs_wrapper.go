package scaling

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"
	config "github.com/bezalel-media-core/v2/configuration"
	"github.com/google/uuid"
)

var ecs_svc = ecs.New(config.GetAwsSession())

func ScaleEcsTasks(clusterName string, desiredRunningTasks int, taskDefinition string) error {
	result, err := ecs_svc.ListTasks(&ecs.ListTasksInput{
		Cluster:       &clusterName,
		DesiredStatus: aws.String(ecs.DesiredStatusRunning),
	})

	if err != nil {
		return err
	}
	desiredTasks := int64(desiredRunningTasks)
	return executeScaling(&clusterName, &desiredTasks, &taskDefinition, result.TaskArns)
}

func executeScaling(clusterName *string, desiredTasks *int64, taskDefinition *string, runningTaskIds []*string) error {
	shouldScaleDown := int64(len(runningTaskIds)) > *desiredTasks
	noScalingRequired := int64(len(runningTaskIds)) == *desiredTasks
	if noScalingRequired {
		log.Printf("no scaling required")
		return nil
	}

	if shouldScaleDown {

		scaleDownDeltaIndex := int64(len(runningTaskIds)) - *desiredTasks
		return stopTasks(clusterName, runningTaskIds[:scaleDownDeltaIndex])
	} else {
		return runTasks(clusterName, desiredTasks, taskDefinition, runningTaskIds)
	}
}

func stopTasks(clusterName *string, runningTaskIds []*string) error {
	for _, t := range runningTaskIds {
		_, err := ecs_svc.StopTask(&ecs.StopTaskInput{
			Cluster: clusterName,
			Reason:  aws.String("SCALE DOWN, ScalerDaemon"),
			Task:    t,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
func runTasks(clusterName *string, desiredTasks *int64, taskDefinition *string, runningTaskIds []*string) error {
	referenceId := uuid.New().String()
	startedBy := "EcsScaleDaemonProcess"
	_, err := ecs_svc.RunTask(&ecs.RunTaskInput{ // TODO: Set as CapacityProviderStrategy for gpu and ec2 general purpose.
		LaunchType:     aws.String(ecs.LaunchTypeFargate), // TODO: Update this for dedicated EC2 launchType. Should automatically launch instances.
		Cluster:        clusterName,
		Count:          desiredTasks,
		TaskDefinition: taskDefinition,
		ReferenceId:    &referenceId,
		StartedBy:      &startedBy,
		NetworkConfiguration: &ecs.NetworkConfiguration{
			AwsvpcConfiguration: &ecs.AwsVpcConfiguration{
				AssignPublicIp: aws.String(ecs.AssignPublicIpEnabled), // This is set for Fargate: pulling private ECR needs public IP. Not sure if needed for dedicated EC2.
				Subnets: []*string{
					aws.String("subnet-0f54e9427db2e5713"), // TODO: move this to env configs.
					aws.String("subnet-03159465d863b0753"),
					aws.String("subnet-0c9a20294b9541bb4"),
				},
			},
		},
	})
	return err
}
