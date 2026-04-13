package barq

import (
	"testing"

	pb "github.com/YASSERRMD/barq-db/barq-sdk-go/proto"
)

func TestInsertStateFromProto(t *testing.T) {
	state := insertStateFromProto(pb.InsertStatusState_INSERT_STATUS_STATE_PROCESSING)
	if state != InsertStateProcessing {
		t.Fatalf("unexpected state %q", state)
	}
}
