/*
 * Copyright 2020 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *
 */

package digitalocean

import (
	"context"
	"fmt"
	"strconv"

	"github.com/digitalocean/godo"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/gardener/external-dns-management/pkg/dns/provider"
	"github.com/gardener/external-dns-management/pkg/dns/provider/raw"
)

type DNSClient struct {
	raw.Executor

	svc godo.DomainsService
	metrics     provider.Metrics
	rateLimiter flowcontrol.RateLimiter
}

func NewDNSClient(apiToken string, metrics provider.Metrics, rateLimiter flowcontrol.RateLimiter) *DNSClient {
	client := godo.NewFromToken(apiToken)
	return &DNSClient{svc: client.Domains, metrics: metrics, rateLimiter: rateLimiter}
}

func (cl *DNSClient) ListDomains() ([]godo.Domain, error) {
	cl.metrics.AddRequests(provider.M_LISTZONES, 1)
	cl.rateLimiter.Accept()

	var domains []godo.Domain
	opt := &godo.ListOptions{
		Page: 1,
		PerPage: 100,
	}
	for {
		doms, resp, err := cl.svc.List(context.Background(), opt)
		if err != nil {
			return nil, err
		}
		domains = append(domains, doms...)

		if resp.Links == nil || resp.Links.IsLastPage() {
			break
		}

		page, err := resp.Links.CurrentPage()
		if err != nil {
			return nil, err
		}
		opt.Page = page + 1
	}

	return domains, nil
}

func (cl *DNSClient) ListRecords(domain string) ([]godo.DomainRecord, error) {
	cl.metrics.AddRequests(provider.M_LISTRECORDS, 1)
	cl.rateLimiter.Accept()

	var records []godo.DomainRecord
	opt := &godo.ListOptions{
		Page: 1,
		PerPage: 100,
	}
	for {
		recs, resp, err := cl.svc.Records(context.Background(), domain, opt)
		if err != nil {
			return nil, err
		}
		records = append(records, recs...)

		if resp.Links == nil || resp.Links.IsLastPage() {
			break
		}

		page, err := resp.Links.CurrentPage()
		if err != nil {
			return nil, err
		}
		opt.Page = page + 1
	}

	return records, nil
}

func (cl *DNSClient) CreateRecord(r raw.Record) error {
	req := createRecordRequest(r)

	cl.metrics.AddRequests(provider.M_CREATERECORDS, 1)
	cl.rateLimiter.Accept()

	if _, _, err := cl.svc.CreateRecord(context.Background(), r.(*Record).domain, req); err != nil {
		return fmt.Errorf("failed to create record of type %q for DNS name %q and value %q: %s", r.GetType(), r.GetDNSName(), r.GetValue(), err)
	}
	return nil
}

func (cl *DNSClient) UpdateRecord(r raw.Record) error {
	req := createRecordRequest(r)

	id, err := strconv.Atoi(r.GetId())
	if err != nil {
		return fmt.Errorf("failed to convert record ID %q to integer: %s", r.GetId(), err)
	}

	cl.metrics.AddRequests(provider.M_UPDATERECORDS, 1)
	cl.rateLimiter.Accept()

	if _, _, err = cl.svc.EditRecord(context.Background(), r.(*Record).domain, id, req); err != nil {
		return fmt.Errorf("failed to update record of type %q for DNS name %q and value %q: %s", r.GetType(), r.GetDNSName(), r.GetValue(), err)
	}
	return nil
}

func (cl *DNSClient) DeleteRecord(r raw.Record) error {
	id, err := strconv.Atoi(r.GetId())
	if err != nil {
		return fmt.Errorf("failed to convert ID %q to integer: %s", r.GetId(), err)
	}

	cl.metrics.AddRequests(provider.M_DELETERECORDS, 1)
	cl.rateLimiter.Accept()

	if _, err = cl.svc.DeleteRecord(context.Background(), r.(*Record).domain, id); err != nil {
		return fmt.Errorf("failed to delete record of type %q for DNS name %q and value %q: %s", r.GetType(), r.GetDNSName(), r.GetValue(), err)
	}
	return nil
}

func (cl *DNSClient) NewRecord(fqdn, rtype, value string, zone provider.DNSHostedZone, ttl int64) raw.Record {
	return toRecord(godo.DomainRecord{
		Type:     rtype,
		Name:     fqdn,
		Data:     value,
		TTL:      int(ttl),
	}, zone.Key())
}

func createRecordRequest(r raw.Record) *godo.DomainRecordEditRequest {
	req := &godo.DomainRecordEditRequest{
		Type: r.GetType(),
		Name: r.GetDNSName(),
		Data: r.GetValue(),
		TTL:  r.GetTTL(),
	}

	if req.TTL < 30 {
		req.TTL = 30
	}

	return req
}
