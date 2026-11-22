/*

 Warpnet - Decentralized Social Network
 Copyright (C) 2025 Vadim Filin, https://github.com/Warp-net,
 <github.com.mecdy@passmail.net>

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.

WarpNet is provided “as is” without warranty of any kind, either expressed or implied.
Use at your own risk. The maintainers shall not be liable for any damages or data loss
resulting from the use or misuse of this software.
*/

// Copyright 2025 Vadim Filin
// SPDX-License-Identifier: AGPL-3.0-or-later

package nostr

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/nostr/nostrtest"
	"github.com/Warp-net/bridge-gateway/internal/warp"
)

type followCall struct {
	route string
	ev    warp.NewFollowEvent
}

// inboundNodes stands in for the watched users' nodes: they list stored
// followers and record follow and reply deliveries.
type inboundNodes struct {
	mu        sync.Mutex
	followers map[string][]string
	calls     []followCall
	replies   []warp.Tweet
	down      bool
}

func (n *inboundNodes) Request(string, any) ([]byte, error) { return nil, errors.New("unused") }

func (n *inboundNodes) RequestUser(userID, route string, payload any) ([]byte, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if route == warp.RouteGetFollowers {
		return json.Marshal(warp.FollowersResponse{FollowingId: userID, Followers: n.followers[userID]})
	}
	if n.down {
		return nil, errors.New("node is offline")
	}
	if reply, ok := payload.(warp.Tweet); ok && route == warp.RoutePostReply {
		n.replies = append(n.replies, reply)
		return json.Marshal(reply)
	}
	n.calls = append(n.calls, followCall{route: route, ev: payload.(warp.NewFollowEvent)})
	return json.Marshal(struct{}{})
}

func (n *inboundNodes) take() []followCall {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := n.calls
	n.calls = nil
	return out
}

func followList(author string, at int64, pks ...string) nostrEvent {
	tags := make([][]string, 0, len(pks))
	for _, pk := range pks {
		tags = append(tags, []string{"p", pk})
	}
	return nostrEvent{ID: hexID(byte(at)), PubKey: author, CreatedAt: at, Kind: kindFollowList, Tags: tags}
}

func newTestInbound(t *testing.T, nodes *inboundNodes, owners ...string) (*nostrInbound, *publishFixture) {
	t.Helper()
	fx := newPublishFixture(t)
	in := newInbound(nodes, fx.pub)
	for _, o := range owners {
		in.add(o)
	}
	return in, fx
}

func TestNostrInboundFollowAndUnfollow(t *testing.T) {
	nodes := &inboundNodes{}
	in, fx := newTestInbound(t, nodes, aliceULID, bobULID)
	alice, bob := fx.pk(t, aliceULID), fx.pk(t, bobULID)
	fan := nostrtest.NewKey().PK

	in.onEvent(followList(fan, 10, alice, "someone-else"))
	calls := nodes.take()
	if len(calls) != 1 || calls[0].route != warp.RoutePostFollow ||
		calls[0].ev.FollowerId != Handle(fan) || calls[0].ev.FollowingId != aliceULID {
		t.Fatalf("follow calls = %+v", calls)
	}

	in.onEvent(followList(fan, 10, alice, "someone-else"))
	if calls := nodes.take(); len(calls) != 0 {
		t.Fatalf("a list already in effect is delivered again: %+v", calls)
	}

	in.onEvent(followList(fan, 20, bob))
	calls = nodes.take()
	if len(calls) != 2 {
		t.Fatalf("calls = %+v, want a follow of bob and an unfollow of alice", calls)
	}
	for _, c := range calls {
		switch c.ev.FollowingId {
		case bobULID:
			if c.route != warp.RoutePostFollow {
				t.Fatalf("bob got %s", c.route)
			}
		case aliceULID:
			if c.route != warp.RoutePostUnfollow {
				t.Fatalf("alice got %s", c.route)
			}
		default:
			t.Fatalf("unexpected call %+v", c)
		}
	}

	in.onEvent(followList(fan, 15, alice))
	if calls := nodes.take(); len(calls) != 0 {
		t.Fatalf("an older list overrides a newer one: %+v", calls)
	}
}

func TestNostrInboundSkipsLockedAndOwnKeys(t *testing.T) {
	nodes := &inboundNodes{}
	in, fx := newTestInbound(t, nodes, aliceULID, carolULID)
	alice := fx.pk(t, aliceULID)
	carol, _ := fx.keys.pubKey(warp.DefaultNetwork, carolULID)

	in.onEvent(followList(nostrtest.NewKey().PK, 10, carol))
	if calls := nodes.take(); len(calls) != 0 {
		t.Fatalf("a locked user got %+v", calls)
	}
	in.onEvent(followList(fx.pk(t, bobULID), 10, alice))
	if calls := nodes.take(); len(calls) != 0 {
		t.Fatalf("a Warpnet user's own list is delivered: %+v", calls)
	}
}

func TestNostrInboundUnfollowOfAStoredFollower(t *testing.T) {
	fan := nostrtest.NewKey().PK
	nodes := &inboundNodes{followers: map[string][]string{aliceULID: {Handle(fan), "01NATIVEFOLLOWER", "x@mastodon.social"}}}
	in, _ := newTestInbound(t, nodes, aliceULID)

	if _, followers := in.keys(); len(followers) != 1 || followers[0] != fan {
		t.Fatalf("followers watched = %v, want only the Nostr one from the node", followers)
	}
	in.onEvent(followList(fan, 10))
	calls := nodes.take()
	if len(calls) != 1 || calls[0].route != warp.RoutePostUnfollow || calls[0].ev.FollowingId != aliceULID {
		t.Fatalf("calls = %+v", calls)
	}
	if _, followers := in.keys(); len(followers) != 0 {
		t.Fatalf("followers watched = %v after the unfollow", followers)
	}
}

func TestNostrInboundRetriesAFailedDelivery(t *testing.T) {
	nodes := &inboundNodes{down: true}
	in, fx := newTestInbound(t, nodes, aliceULID)
	fan := nostrtest.NewKey().PK
	list := followList(fan, 10, fx.pk(t, aliceULID))

	in.onEvent(list)
	nodes.mu.Lock()
	nodes.down = false
	nodes.mu.Unlock()
	in.onEvent(list)
	if calls := nodes.take(); len(calls) != 1 || calls[0].route != warp.RoutePostFollow {
		t.Fatalf("calls = %+v, want the follow delivered on the repeat", calls)
	}
}

func TestNostrInboundReachesAUserAddedLater(t *testing.T) {
	nodes := &inboundNodes{}
	in, fx := newTestInbound(t, nodes, aliceULID)
	fan := nostrtest.NewKey().PK
	list := followList(fan, 10, fx.pk(t, aliceULID), fx.pk(t, bobULID))

	in.onEvent(list)
	if calls := nodes.take(); len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	in.add(bobULID)
	in.onEvent(list)
	calls := nodes.take()
	if len(calls) != 1 || calls[0].ev.FollowingId != bobULID {
		t.Fatalf("calls = %+v, want only bob's follow", calls)
	}
}

func TestNostrInboundDeliversADirectReply(t *testing.T) {
	nodes := &inboundNodes{}
	in, fx := newTestInbound(t, nodes, aliceULID)
	alice := fx.pk(t, aliceULID)
	fx.pub.PublishPost(aliceULID, warp.Tweet{Id: "TWEET1", RootId: "TWEET1", UserId: aliceULID, Text: "hello nostr"})
	published := fx.relay.Published(alice, KindNote)
	if len(published) != 1 {
		t.Fatalf("published = %d notes, want 1", len(published))
	}
	post := published[0].ID.Hex()
	fan := nostrtest.NewKey()
	now := time.Now().Unix()

	reply := fromLibEvent(fan.Sign(t, KindNote, "nice post", now, []string{"e", post, "", "root"}, []string{"p", alice}))
	in.onReply(reply)
	if len(nodes.replies) != 1 {
		t.Fatalf("replies = %+v, want one", nodes.replies)
	}
	got := nodes.replies[0]
	if got.Id != reply.ID || got.UserId != Handle(fan.PK) || got.Text != "nice post" ||
		got.ParentId == nil || *got.ParentId != "TWEET1" || got.ParentUserId == nil || *got.ParentUserId != aliceULID ||
		got.RootId != "TWEET1" {
		t.Fatalf("reply = %+v", got)
	}

	nodes.replies = nil
	stranger := fromLibEvent(fan.Sign(t, KindNote, "answering someone else", now, []string{"e", fx.note.ID.Hex(), "", "root"}, []string{"p", alice}))
	in.onReply(stranger)
	mention := fromLibEvent(fan.Sign(t, KindNote, "just a mention", now, []string{"p", alice}))
	in.onReply(mention)
	if len(nodes.replies) != 0 {
		t.Fatalf("a note that does not answer alice's tweet is delivered: %+v", nodes.replies)
	}
}
