package internal

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type gateAction int

const (
	gateWait gateAction = iota
	gateUnsuspend
	gateBlock
)

func listResourceBindings(ctx context.Context, c client.Client) ([]*unstructured.Unstructured, error) {
	// MigrationRestore와 같은 패턴:
	// - unstructured list
	// - GVK를 work.karmada.io/v1alpha2 ResourceBindingList로 지정
	rbList := &unstructured.UnstructuredList{}
	rbList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "work.karmada.io",
		Version: "v1alpha2",
		Kind:    "ResourceBindingList",
	})

	if err := c.List(ctx, rbList); err != nil {
		return nil, err
	}

	out := make([]*unstructured.Unstructured, 0, len(rbList.Items))
	for i := range rbList.Items {
		out = append(out, &rbList.Items[i])
	}
	return out, nil
}

// MigrationRestore의 dual-path 방식 그대로:
// - spec.suspension.suspension.dispatching
// - spec.suspension.dispatching
func isRBSuspendedU(rb *unstructured.Unstructured) bool {
	if v, found, _ := unstructured.NestedBool(rb.Object, "spec", "suspension", "suspension", "dispatching"); found {
		return v
	}
	if v, found, _ := unstructured.NestedBool(rb.Object, "spec", "suspension", "dispatching"); found {
		return v
	}
	return false
}

func shouldUnsuspendByGates(rb *unstructured.Unstructured) (gateAction, string) {
	ann := rb.GetAnnotations()
	if ann == nil {
		return gateWait, "no annotations"
	}

	restorePhase := ann[AnnoRestorePhase]
	pvRestore := ann[AnnPVRestore]

	// ✅ 핵심 의도: annotation 키가 없으면 절대 풀지 않고 대기
	if restorePhase == "" || pvRestore == "" {
		return gateWait, fmt.Sprintf("missing annotation(s): %s=%t %s=%t",
			AnnoRestorePhase, restorePhase != "",
			AnnPVRestore, pvRestore != "",
		)
	}

	// 실패 차단
	if restorePhase == RestoreFailed || pvRestore == PVError {
		return gateBlock, fmt.Sprintf("blocked: %s=%s %s=%s", AnnoRestorePhase, restorePhase, AnnPVRestore, pvRestore)
	}

	// 완료/none 조합만 통과
	okRestore := (restorePhase == RestoreSucceeded || restorePhase == RestoreNone)
	okPV := (pvRestore == PVCompleted || pvRestore == PVNone)

	if okRestore && okPV {
		return gateUnsuspend, fmt.Sprintf("gates satisfied: %s=%s %s=%s", AnnoRestorePhase, restorePhase, AnnPVRestore, pvRestore)
	}

	// processing/working 등은 그냥 대기
	return gateWait, fmt.Sprintf("not ready: %s=%s %s=%s", AnnoRestorePhase, restorePhase, AnnPVRestore, pvRestore)
}

func unsuspendRB(ctx context.Context, c client.Client, rb *unstructured.Unstructured) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		before := &unstructured.Unstructured{}
		before.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "work.karmada.io",
			Version: "v1alpha2",
			Kind:    "ResourceBinding",
		})

		if err := c.Get(ctx, client.ObjectKey{Namespace: rb.GetNamespace(), Name: rb.GetName()}, before); err != nil {
			return err
		}

		// 이미 풀렸으면 no-op
		if !isRBSuspendedU(before) {
			return nil
		}

		after := before.DeepCopy()

		// 존재하는 경로를 우선으로 false 설정
		if _, found, _ := unstructured.NestedBool(after.Object, "spec", "suspension", "suspension", "dispatching"); found {
			_ = unstructured.SetNestedField(after.Object, false, "spec", "suspension", "suspension", "dispatching")
		} else {
			_ = unstructured.SetNestedField(after.Object, false, "spec", "suspension", "dispatching")
		}

		return c.Patch(ctx, after, client.MergeFrom(before))
	})
}
