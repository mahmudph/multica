"use client";

import { useEffect } from "react";
import { useTheme } from "@multica/ui/components/common/theme-provider";

// Status bar / task-switcher color for installed PWAs (Android) and Safari's
// browser chrome. The static `<meta name="theme-color">` pair emitted from
// `viewport.themeColor` in app/layout.tsx only reacts to the OS-level
// `prefers-color-scheme` media query. But the app's theme is user-overridable
// independently of the OS setting (Settings > Preferences > Theme, backed by
// next-themes' `enableSystem`), so a user who picks Dark while their phone
// stays in Light mode (or vice versa) gets a status bar color that doesn't
// match the actual rendered theme — on Android the icon color is computed
// from that mismatched background and can end up unreadable.
//
// This keeps both emitted `<meta name="theme-color">` tags in sync with the
// theme next-themes actually resolved, overriding whichever one the media
// query picked.
const LIGHT_COLOR = "#ffffff";
const DARK_COLOR = "#05070b";

export function ThemeColorMeta() {
  const { resolvedTheme } = useTheme();

  useEffect(() => {
    const color = resolvedTheme === "dark" ? DARK_COLOR : LIGHT_COLOR;
    document
      .querySelectorAll('meta[name="theme-color"]')
      .forEach((meta) => meta.setAttribute("content", color));
  }, [resolvedTheme]);

  return null;
}
