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
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Warp-net/bridge-gateway/internal/warp"
)

// nostrInbound delivers Nostr follows of consenting Warpnet users to their
// nodes, as the fediverse inbox does for Mastodon: a kind 3 list naming a
// user's key is a follow, the author's next list without it an unfollow. The
// followers already stored on a node are read back when its user is added, so
// the gateway keeps no state of its own.

const (
	nostrInboundMaxKeys     = 1000
	nostrInboundRefresh     = 30 * time.Minute
	nostrInboundPageSize    = uint64(100)
	nostrInboundMaxPages    = 100
	nostrInboundStoredLists = 1 // asks relays for stored lists too, not only new ones
)

type nostrInbound struct {
	req warp.Requester
	pub *Publisher

	mu        sync.Mutex
	owners    map[string]string              // Warpnet user's key -> user id
	ownerKeys map[string]string              // user id -> key
	followers map[string]map[string]struct{} // follower key -> followed user ids
	applied   map[string]int64               // follower key -> created_at of the list applied
	changed   chan struct{}

	resubscribeDelay time.Duration
}

func newInbound(req warp.Requester, pub *Publisher) *nostrInbound {
	return &nostrInbound{
		req:              req,
		pub:              pub,
		owners:           map[string]string{},
		ownerKeys:        map[string]string{},
		followers:        map[string]map[string]struct{}{},
		applied:          map[string]int64{},
		changed:          make(chan struct{}, 1),
		resubscribeDelay: nostrResubscribeDelay,
	}
}

// add watches a consenting user's key and loads their Nostr followers from
// their node.
func (in *nostrInbound) add(owner string) {
	pk, ok := in.pub.keys.pubKey(in.pub.network, owner)
	if !ok {
		return
	}
	stored := in.storedFollowers(owner)
	in.mu.Lock()
	in.owners[pk], in.ownerKeys[owner] = owner, pk
	for _, fpk := range stored {
		in.addFollower(fpk, owner)
	}
	in.mu.Unlock()
	in.notify()
}

func (in *nostrInbound) storedFollowers(owner string) []string {
	limit := nostrInboundPageSize
	var cursor string
	var out []string
	for range nostrInboundMaxPages {
		ev := warp.GetFollowersEvent{UserId: owner, Limit: &limit}
		if cursor != "" {
			ev.Cursor = &cursor
		}
		bt, err := in.req.RequestUser(owner, warp.RouteGetFollowers, ev)
		if err != nil || warp.NodeResponseError(bt) != nil {
			return out
		}
		var resp warp.FollowersResponse
		if json.Unmarshal(bt, &resp) != nil {
			return out
		}
		for _, id := range resp.Followers {
			if pk, ok := nostrPubKey(id); ok {
				out = append(out, pk)
			}
		}
		if len(resp.Followers) == 0 || resp.Cursor == "" || resp.Cursor == cursor {
			return out
		}
		cursor = resp.Cursor
	}
	return out
}

// addFollower reports whether fpk is a new follower key.
func (in *nostrInbound) addFollower(fpk, owner string) bool {
	fs := in.followers[fpk]
	if fs == nil {
		fs = map[string]struct{}{}
		in.followers[fpk] = fs
	}
	fs[owner] = struct{}{}
	return len(fs) == 1
}

// removeFollower reports whether fpk follows no watched user any more.
func (in *nostrInbound) removeFollower(fpk, owner string) bool {
	delete(in.followers[fpk], owner)
	if len(in.followers[fpk]) == 0 {
		delete(in.followers, fpk)
		return true
	}
	return false
}

func (in *nostrInbound) notify() {
	select {
	case in.changed <- struct{}{}:
	default:
	}
}

func (in *nostrInbound) keys() (owners, followers []string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	for pk := range in.owners {
		if len(owners) == nostrInboundMaxKeys {
			break
		}
		owners = append(owners, pk)
	}
	for pk := range in.followers {
		if len(followers) == nostrInboundMaxKeys {
			break
		}
		followers = append(followers, pk)
	}
	return owners, followers
}

// Run keeps two subscriptions on the default relays: lists naming a watched
// user, which bring follows, and the known followers' lists, which bring
// unfollows. Both are re-opened whenever either set changes.
func (in *nostrInbound) Run(ctx context.Context) {
	for {
		owners, followers := in.keys()
		subCtx, cancel := context.WithCancel(ctx)
		relays := in.pub.bridge.defaults
		if len(owners) > 0 {
			go in.pub.bridge.relays.subscribe(subCtx, relays, nostrFilter{
				Kinds: []int{kindFollowList}, Tags: map[string][]string{"p": owners}, Since: nostrInboundStoredLists,
			}, in.onEvent)
		}
		if len(followers) > 0 {
			go in.pub.bridge.relays.subscribe(subCtx, relays, nostrFilter{
				Kinds: []int{kindFollowList}, Authors: followers, Since: nostrInboundStoredLists,
			}, in.onEvent)
		}
		select {
		case <-ctx.Done():
			cancel()
			return
		case <-in.changed:
			select {
			case <-ctx.Done():
			case <-time.After(in.resubscribeDelay):
			}
		case <-time.After(nostrInboundRefresh):
		}
		cancel()
	}
}

// onEvent applies an author's newest follow list to the watched users. The
// same list arriving again is applied again: it reaches users added since and
// retries a failed delivery, while a list already in effect changes nothing.
func (in *nostrInbound) onEvent(ev nostrEvent) {
	if ev.Kind != kindFollowList {
		return
	}
	if _, own := in.pub.keys.owner(ev.PubKey); own {
		return // a Warpnet user's own list; their follows are native
	}
	tagged := map[string]bool{}
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "p" {
			tagged[tag[1]] = true
		}
	}
	in.mu.Lock()
	if ev.CreatedAt < in.applied[ev.PubKey] {
		in.mu.Unlock()
		return
	}
	in.applied[ev.PubKey] = ev.CreatedAt
	var follows, unfollows []string
	for pk := range tagged {
		if owner, ok := in.owners[pk]; ok {
			if _, had := in.followers[ev.PubKey][owner]; !had {
				follows = append(follows, owner)
			}
		}
	}
	for owner := range in.followers[ev.PubKey] {
		if !tagged[in.ownerKeys[owner]] {
			unfollows = append(unfollows, owner)
		}
	}
	in.mu.Unlock()

	keysChanged := false
	for _, owner := range follows {
		if !in.pub.consents(owner) {
			continue
		}
		if !in.deliver(owner, ev.PubKey, false) {
			continue
		}
		in.mu.Lock()
		keysChanged = in.addFollower(ev.PubKey, owner) || keysChanged
		in.mu.Unlock()
	}
	for _, owner := range unfollows {
		if !in.deliver(owner, ev.PubKey, true) {
			continue
		}
		in.mu.Lock()
		keysChanged = in.removeFollower(ev.PubKey, owner) || keysChanged
		in.mu.Unlock()
	}
	if keysChanged {
		in.notify() // the followers' subscription must cover the new key set
	}
}

func (in *nostrInbound) deliver(owner, follower string, undo bool) bool {
	route := warp.RoutePostFollow
	if undo {
		route = warp.RoutePostUnfollow
	}
	bt, err := in.req.RequestUser(owner, route, warp.NewFollowEvent{FollowerId: Handle(follower), FollowingId: owner})
	if err == nil {
		err = warp.NodeResponseError(bt)
	}
	if err != nil {
		warp.NetLog(in.pub.network).Warnf("nostr: delivering %s of %s by %s: %v", route, owner, follower, err)
		return false
	}
	warp.NetLog(in.pub.network).Infof("nostr: delivered %s of %s by %s", route, owner, follower)
	return true
}
