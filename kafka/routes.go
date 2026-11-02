package kafka

import (
	"fmt"
	"strings"
)

type Route struct {
	Type    string `json:"type"`
	Version string `json:"version"`
	Topic   string `json:"topic"`
}

type routeKey struct {
	typeName string
	version  string
}

type Router struct {
	routes map[routeKey]string
}

func NewRouter(routes []Route) (Router, error) {
	r := Router{routes: make(map[routeKey]string, len(routes))}
	if len(routes) == 0 {
		return r, fmt.Errorf("kafka: at least one route is required")
	}
	for _, route := range routes {
		t := strings.TrimSpace(route.Type)
		v := strings.TrimSpace(route.Version)
		topic := strings.TrimSpace(route.Topic)
		if t == "" || v == "" || topic == "" {
			return r, fmt.Errorf("kafka: route type, version and topic are required")
		}
		key := routeKey{typeName: t, version: v}
		if _, exists := r.routes[key]; exists {
			return r, fmt.Errorf("kafka: duplicate route for %s/%s", t, v)
		}
		r.routes[key] = topic
	}
	return r, nil
}

func (r Router) Resolve(typeName, version string) (string, error) {
	topic, ok := r.routes[routeKey{typeName: strings.TrimSpace(typeName), version: strings.TrimSpace(version)}]
	if !ok {
		return "", fmt.Errorf("%w: %s/%s", ErrNoRoute, typeName, version)
	}
	return topic, nil
}

func (r Router) Topics() []string {
	seen := make(map[string]struct{}, len(r.routes))
	out := make([]string, 0, len(r.routes))
	for _, topic := range r.routes {
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		out = append(out, topic)
	}
	return out
}
