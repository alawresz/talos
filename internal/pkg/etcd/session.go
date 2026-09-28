// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package etcd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cosi-project/runtime/pkg/state"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// CleanupTimeout bounds the best-effort cleanup RPCs issued when an etcd lock is released or an
// election campaign is torn down.
//
// The cleanup runs on a context detached from the caller's one, as the caller's context is
// usually already canceled at that point (that is what aborted the lock or the campaign), but it
// must never block indefinitely: on machine shutdown the local etcd is stopped before the
// controller runtime is torn down, so the cleanup RPCs have no chance to ever complete.
const CleanupTimeout = 10 * time.Second

// revokeAttemptTimeout bounds a single revoke attempt in RevokeSessionThroughControlPlane.
//
// A proposal dropped by an etcd leader change waits for the whole etcd request timeout (5s plus
// twice the election timeout), which can outlast CleanupTimeout, while a retry succeeds at once.
const revokeAttemptTimeout = time.Second

// RevokeSession stops refreshing the session lease and revokes it on a context detached from ctx.
//
// This is what `concurrency.Session.Close` does, but with a context under our control:
// `Session.Close` revokes the lease on the session context, which defaults to the etcd client
// context, so it fails instantly once that context is canceled, and otherwise blocks for the
// whole session TTL (60s) when etcd is unreachable. Neither is useful: the point of the revoke is
// to run exactly when the caller is going away.
//
// Revoking the lease drops every key attached to it, which for the `concurrency` package means
// the mutex and election keys of this session. Both are FIFO queues ordered by create revision,
// so a key left behind by a participant which is already gone blocks everybody queued behind it
// until the lease expires on its own.
func (c *Client) RevokeSession(ctx context.Context, session *concurrency.Session) error {
	// stop refreshing the lease; this is local-only and doesn't talk to etcd
	session.Orphan()

	revokeCtx, revokeCancel := context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
	defer revokeCancel()

	if _, err := c.Revoke(revokeCtx, session.Lease()); err != nil {
		return fmt.Errorf("error revoking etcd session lease: %w", err)
	}

	return nil
}

// RevokeSessionThroughControlPlane revokes the session lease through the control plane members.
//
// It is for the case when the local etcd member is gone (for example, after `etcd leave`), as the
// session's own client talks to the local member only and can't reach the cluster anymore.
func RevokeSessionThroughControlPlane(ctx context.Context, st state.State, session *concurrency.Session) error {
	// stop refreshing the lease; this is local-only and doesn't talk to etcd
	session.Orphan()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
	defer cancel()

	client, err := NewClientFromControlPlaneIPs(ctx, st)
	if err != nil {
		return fmt.Errorf("error creating etcd client: %w", err)
	}

	defer client.Close() //nolint:errcheck

	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, revokeAttemptTimeout)
		_, err = client.Revoke(attemptCtx, session.Lease())

		attemptCancel()

		// a retried revoke which was already applied reports the lease as not found
		if err == nil || errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return nil
		}

		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return fmt.Errorf("error revoking etcd session lease: %w", err)
		}
	}
}
