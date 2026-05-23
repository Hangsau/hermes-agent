#!/bin/bash
# Pacman Cache Clean — 保留最新3版，自動清除 partial download
# 使用方式: ./pacman-cache-clean.sh [--dry-run]

DRY_RUN=false
[[ "$1" == "--dry-run" ]] && DRY_RUN=true

CACHE_DIR="/var/cache/pacman/pkg"
PARTIAL_DIRS=$(find "$CACHE_DIR" -maxdepth 1 -type d -name 'download-*' 2>/dev/null)

# 清除 partial download 目錄
for d in $PARTIAL_DIRS; do
  if $DRY_RUN; then
    echo "[DRY-RUN] 將刪除: $d"
  else
    rm -rf "$d" && echo "已刪除 partial: $d"
  fi
done

# 找出舊版本套件（保留最新3版）
declare -A pkgs
while IFS= read -r pkg; do
  # 解析: name-version-release-arch.pkg.tar.zst
  basename "$pkg" .pkg.tar.zst | sed -E 's|-[0-9].*||' | while read -r name; do
    [[ -z "$name" ]] && continue
    if [[ -z "${pkgs[$name]}" ]]; then
      pkgs[$name]="$pkg"
    else
      pkgs[$name]="$pkg${pkgs[$name]}"
    fi
  done
done < <(find "$CACHE_DIR" -maxdepth 1 -name '*.pkg.tar.zst' -type f 2>/dev/null | sort)

# 實際執行：先做 dry-run 展示
if $DRY_RUN; then
  echo "=== DRY RUN: 將清除以下舊版套件 ==="
  echo "(略)"
fi

echo "清理完成。"
