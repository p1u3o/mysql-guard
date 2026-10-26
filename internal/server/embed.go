package server

import "embed"

// Static UI assets (no CDN, no build step).
//
//go:embed static/*
var staticFS embed.FS
