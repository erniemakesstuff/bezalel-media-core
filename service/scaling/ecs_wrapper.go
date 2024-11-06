package scaling

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"
	config "github.com/bezalel-media-core/v2/configuration"
	"github.com/google/uuid"
)

var ecs_svc = ecs.New(config.GetAwsSession())

func scaleTask(clusterName string, desiredRunningTasks int, taskDefinition string) error {
	const MAX_TASKS = 20
	result, err := ecs_svc.ListTasks(&ecs.ListTasksInput{
		Cluster:       &clusterName,
		DesiredStatus: aws.String(ecs.DesiredStatusRunning),
	})

	if err != nil {
		return err
	}
	runningTasks := len(result.TaskArns)
	insufficientTasks := runningTasks < desiredRunningTasks
	withinTaskLimit := desiredRunningTasks+runningTasks < MAX_TASKS
	desiredTasksDelta := int64(desiredRunningTasks) - int64(runningTasks)
	if desiredTasksDelta < 0 { // indicates more tasks running than desired; scale to 0.
		desiredTasksDelta = 0
	}

	if insufficientTasks && withinTaskLimit {
		return scaleEcsCluster(&clusterName, &desiredTasksDelta, &taskDefinition, result.TaskArns)
	} else {
		log.Printf("unable to scale, withinTaskLimit %t insufficientTasks %t", withinTaskLimit, insufficientTasks)
	}
	return nil
}

func scaleEcsCluster(clusterName *string, desiredTasks *int64, taskDefinition *string, runningTaskIds []*string) error {
	// TODO:
	return runTasks(clusterName, desiredTasks, taskDefinition, runningTaskIds)
}
func stopTask(taskId string) error {
	return nil
}
func runTasks(clusterName *string, desiredTasks *int64, taskDefinition *string, runningTaskIds []*string) error {
	referenceId := uuid.New().String()
	startedBy := "EcsScaleDaemonProcess"
	_, err := ecs_svc.RunTask(&ecs.RunTaskInput{
		LaunchType:     aws.String(ecs.LaunchTypeFargate), // TODO: Update this for dedicated EC2 launchType.
		Cluster:        clusterName,
		Count:          desiredTasks,
		TaskDefinition: taskDefinition,
		ReferenceId:    &referenceId,
		StartedBy:      &startedBy,
		NetworkConfiguration: &ecs.NetworkConfiguration{
			AwsvpcConfiguration: &ecs.AwsVpcConfiguration{
				AssignPublicIp: aws.String(ecs.AssignPublicIpEnabled), // This is set for Fargate: pulling private ECR needs public IP. Not sure if needed for dedicated EC2.
				Subnets: []*string{
					aws.String("subnet-0f54e9427db2e5713"),
					aws.String("subnet-03159465d863b0753"),
					aws.String("subnet-0c9a20294b9541bb4"),
				},
			},
		},
	})
	return err
}
