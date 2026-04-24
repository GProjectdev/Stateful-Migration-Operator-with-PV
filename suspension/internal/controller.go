package internal

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type SuspensionPoller struct {
	KarmadaClient client.Client
	ScanInterval  time.Duration
}

func (p *SuspensionPoller) SetupWithManager(mgr manager.Manager) error {
	if p.ScanInterval <= 0 {
		p.ScanInterval = 5 * time.Second
	}

	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(p.ScanInterval)
		defer ticker.Stop()

		klog.Infof("suspension-controller poller started (interval=%s)", p.ScanInterval)

		for {
			select {
			case <-ctx.Done():
				klog.Info("suspension-controller poller stopping")
				return nil
			case <-ticker.C:
				p.scanOnce(ctx)
			}
		}
	}))
}

func (p *SuspensionPoller) scanOnce(ctx context.Context) {
	rbs, err := listResourceBindings(ctx, p.KarmadaClient)
	if err != nil {
		klog.Errorf("list ResourceBindings failed: %v", err)
		return
	}

	total := len(rbs)
	suspended := 0
	wait := 0
	blocked := 0
	unsuspended := 0

	for i := range rbs {
		rb := rbs[i]

		// 1) RB가 실제로 suspended 상태여야만 의미 있음
		if !isRBSuspendedU(rb) {
			continue
		}
		suspended++

		// 2) gate(annotations) 만족해야 함 (없으면 무조건 WAIT)
		action, reason := shouldUnsuspendByGates(rb)

		switch action {
		case gateWait:
			wait++
			klog.V(2).Infof("WAIT %s/%s: %s", rb.GetNamespace(), rb.GetName(), reason)
			
		case gateBlock:
			// failed/error면 풀지 않음
			blocked++
			klog.Infof("BLOCK %s/%s: %s", rb.GetNamespace(), rb.GetName(), reason)
		case gateUnsuspend:
			if err := unsuspendRB(ctx, p.KarmadaClient, rb); err != nil {
				klog.Errorf("unsuspend RB %s/%s failed: %v", rb.GetNamespace(), rb.GetName(), err)
				continue
			}
			unsuspended++
			klog.Infof("UNSUSPEND %s/%s: %s", rb.GetNamespace(), rb.GetName(), reason)
		}
	}

	// 🔥 핵심: unsuspend가 없어도 항상 요약 로그 출력
	klog.Infof(
		"suspension scan summary: total=%d suspended=%d wait=%d blocked=%d unsuspended=%d",
		total, suspended, wait, blocked, unsuspended,
	)
}
