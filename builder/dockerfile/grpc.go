package dockerfile

import (
	"io"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/builder"
	"github.com/docker/docker/builder/dockerfile/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (bm *BuildManager) BuildServer(ctx context.Context) http.Handler {
	gs := grpc.NewServer()
	bs := &dockerfileService{
		manager: bm,
	}
	api.RegisterDockerfileServiceServer(gs, bs)
	return gs
}

type dockerfileService struct {
	manager *BuildManager
}

func (s *dockerfileService) SendContext(ctx api.DockerfileService_SendContextServer) error {
	logrus.Debugf("Context called: %#v", ctx)

	req, err := ctx.Recv()
	if err != nil {
		return err
	}

	writeDone := make(chan struct{})
	pr, pw := io.Pipe()

	go func() {
		var err error
		for {
			_, err = pw.Write(req.TarContent)
			if err != nil {
				break
			}
			req, err = ctx.Recv()
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}

		}
		if err := pw.CloseWithError(err); err != nil {
			logrus.Errorf("Failed to close tar transfer pipe")
		}

		close(writeDone)
	}()

	if err := builder.AttachSession(pr, req.SessionID); err != nil {
		return err
	}

	<-writeDone

	return ctx.SendAndClose(&api.ContextResponse{})
}
