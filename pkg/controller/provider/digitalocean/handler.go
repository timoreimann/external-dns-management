/*
 * Copyright 2020 SAP SE or an SAP affiliate company. All rights reserved. h file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
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
	"github.com/gardener/controller-manager-library/pkg/logger"

	"github.com/gardener/external-dns-management/pkg/dns"
	"github.com/gardener/external-dns-management/pkg/dns/provider"
	"github.com/gardener/external-dns-management/pkg/dns/provider/raw"
)

type Handler struct {
	provider.DefaultDNSHandler
	config provider.DNSHandlerConfig
	cache  provider.ZoneCache
	client *DNSClient
}

var _ provider.DNSHandler = &Handler{}

func NewHandler(c *provider.DNSHandlerConfig) (provider.DNSHandler, error) {
	var err error

	h := &Handler{
		DefaultDNSHandler: provider.NewDefaultDNSHandler(ProviderTypeDigitalOcean),
		config:            *c,
	}

	apiToken, err := c.GetRequiredProperty("DIGITALOCEAN_ACCESS_TOKEN", "apiToken")
	if err != nil {
		return nil, err
	}

	h.client = NewDNSClient(apiToken, c.Metrics, c.RateLimiter)

	h.cache, err = provider.NewZoneCache(*c.CacheConfig.CopyWithDisabledZoneStateCache(), c.Metrics, nil, h.getZones, h.getZoneState)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *Handler) Release() {
	h.cache.Release()
}

func (h *Handler) GetZones() (provider.DNSHostedZones, error) {
	return h.cache.GetZones()
}

func (h *Handler) getZones(_ provider.ZoneCache) (provider.DNSHostedZones, error) {
	domains, err := h.client.ListDomains()
	if err != nil {
		return nil, err
	}

	zones := provider.DNSHostedZones{}

	var forwarded []string
	for _, dom := range domains {
		records, err := h.client.ListRecords(dom.Name)
		if err != nil {
			return nil, err
		}

		for _, record := range records {
			if record.Type == dns.RS_NS && record.Name != dom.Name {
				forwarded = append(forwarded, record.Name)
			}
		}

		hostedZone := provider.NewDNSHostedZone(h.ProviderType(), dom.Name, dom.Name, dom.Name, forwarded, false)
		zones = append(zones, hostedZone)
	}

	return zones, nil
}

func (h *Handler) GetZoneState(zone provider.DNSHostedZone) (provider.DNSZoneState, error) {
	return h.cache.GetZoneState(zone)
}

func (h *Handler) getZoneState(zone provider.DNSHostedZone, _ provider.ZoneCache) (provider.DNSZoneState, error) {
	records, err := h.client.ListRecords(zone.Domain())
	if err != nil {
		return nil, err
	}

	state := raw.NewState()
	for _, record := range records {
		r := toRecord(record, zone.Domain())
		state.AddRecord(r)
	}
	state.CalculateDNSSets()

	return state, nil
}

func (h *Handler) ReportZoneStateConflict(zone provider.DNSHostedZone, err error) bool {
	return h.cache.ReportZoneStateConflict(zone, err)
}

func (h *Handler) ExecuteRequests(logger logger.LogContext, zone provider.DNSHostedZone, state provider.DNSZoneState, reqs []*provider.ChangeRequest) error {
	err := raw.ExecuteRequests(logger, &h.config, h.client, zone, state, reqs)
	h.cache.ApplyRequests(err, zone, reqs)
	return err
}
