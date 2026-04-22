#!/bin/bash
# Move audio from SRC (recursive) to DST (flat), converting FLAC -> WAV.
# Source is deleted only after the destination write succeeds.
set -u
set -o pipefail

SRC="/run/media/dixi/BLJA/1/old"
DST="/run/media/dixi/BLJA/1/new"
BAR_WIDTH=30

# ---- Preflight ----
command -v ffmpeg >/dev/null 2>&1 || { echo "ffmpeg is required but not installed" >&2; exit 1; }
[[ -d "$SRC" ]] || { echo "Source directory not found: $SRC" >&2; exit 1; }
mkdir -p "$DST"

SRC_ABS=$(cd -- "$SRC" && pwd)
DST_ABS=$(cd -- "$DST" && pwd)
if [[ "$DST_ABS" == "$SRC_ABS" || "$DST_ABS" == "$SRC_ABS"/* ]]; then
    echo "Error: destination is equal to or inside source. Refusing to run." >&2
    exit 1
fi

# ---- Interrupt handling ----
# Track any file currently being written so it can be cleaned up on signal.
current_partial=""
cleanup_and_exit() {
    if [[ -n "$current_partial" && -e "$current_partial" ]]; then
        rm -f -- "$current_partial"
    fi
    printf '\n'
    exit 130
}
trap cleanup_and_exit INT TERM HUP

# Size the message portion of the progress line to the terminal
TERM_COLS=$(tput cols 2>/dev/null || echo 80)
MSG_MAX=$(( TERM_COLS - BAR_WIDTH - 35 ))
(( MSG_MAX < 10 )) && MSG_MAX=10

# ---- Helpers ----
file_size() {
    local s
    s=$(stat -c %s -- "$1" 2>/dev/null) && [[ -n "$s" ]] && { echo "$s"; return; }
    s=$(stat -f %z -- "$1" 2>/dev/null) && [[ -n "$s" ]] && { echo "$s"; return; }
    wc -c < "$1" 2>/dev/null | tr -d ' '
}

fmt_time() {
    local s=$1
    if (( s < 0 )); then printf -- '--:--'; return; fi
    local h=$((s/3600)) m=$(((s%3600)/60)) sec=$((s%60))
    if (( h > 0 )); then
        printf '%d:%02d:%02d' "$h" "$m" "$sec"
    else
        printf '%02d:%02d' "$m" "$sec"
    fi
}

draw_progress() {
    local cur=$1 total=$2 elapsed=$3 eta=$4 msg=$5
    local pct=0 filled=0
    if (( total > 0 )); then
        pct=$(( cur * 100 / total ))
        filled=$(( cur * BAR_WIDTH / total ))
    fi
    local bar empty
    printf -v bar   '%*s' "$filled"              ''; bar="${bar// /#}"
    printf -v empty '%*s' "$((BAR_WIDTH-filled))" ''; empty="${empty// /-}"

    if (( ${#msg} > MSG_MAX )); then msg="...${msg: -$((MSG_MAX-3))}"; fi
    printf '\r\033[K[%s%s] %d/%d (%3d%%) %s<%s %s' \
        "$bar" "$empty" "$cur" "$total" "$pct" \
        "$(fmt_time "$elapsed")" "$(fmt_time "$eta")" "$msg"
}

# Take the first line of a (possibly multi-line) string, strip CRs.
first_line() {
    local s=${1%%$'\n'*}
    printf '%s' "${s//$'\r'/}"
}

# ---- Collect ----
mapfile -d '' files < <(find "$SRC" -type f \( \
    -iname '*.flac' -o -iname '*.wav'  -o -iname '*.mp3'  -o \
    -iname '*.m4a'  -o -iname '*.aac'  -o -iname '*.ogg'  -o \
    -iname '*.oga'  -o -iname '*.opus' -o -iname '*.wma'  -o \
    -iname '*.aiff' -o -iname '*.aif'  -o -iname '*.ape'  -o \
    -iname '*.wv'   -o -iname '*.alac' \
\) -print0)

total=${#files[@]}
if (( total == 0 )); then
    echo "No audio files found under $SRC"
    exit 0
fi

echo "Found $total audio files. Sizing..."
total_bytes=0
declare -a sizes=()
for f in "${files[@]}"; do
    sz=$(file_size "$f"); [[ "$sz" =~ ^[0-9]+$ ]] || sz=0
    sizes+=("$sz")
    total_bytes=$((total_bytes + sz))
done
echo "Total: $((total_bytes / 1024 / 1024)) MB across $total files. Starting..."

# ---- Process ----
converted=0; moved=0; failed=0
errors=()
done_bytes=0
done_count=0
SECONDS=0

for idx in "${!files[@]}"; do
    src="${files[idx]}"
    sz="${sizes[idx]}"
    base=$(basename -- "$src")
    name="${base%.*}"
    ext="${base##*.}"
    ext_lc="${ext,,}"

    if [[ "$ext_lc" == "flac" ]]; then target_ext="wav"; else target_ext="$ext_lc"; fi

    target="$DST/$name.$target_ext"
    i=1
    while [[ -e "$target" ]]; do
        target="$DST/${name}_$i.$target_ext"
        i=$((i+1))
    done

    elapsed=$SECONDS
    eta=-1
    if (( elapsed >= 2 && done_bytes > 0 )); then
        eta=$(( (total_bytes - done_bytes) * elapsed / done_bytes ))
    fi

    if [[ "$ext_lc" == "flac" ]]; then
        draw_progress "$done_count" "$total" "$elapsed" "$eta" "Converting: $base"
        current_partial="$target"
        if err=$(ffmpeg -hide_banner -nostdin -loglevel error -n -i "$src" "$target" 2>&1); then
            current_partial=""
            if rm -- "$src" 2>/dev/null; then
                converted=$((converted+1))
                done_bytes=$((done_bytes + sz))
            else
                # Conversion succeeded but we couldn't remove the source.
                # Leave the converted file in place and flag it.
                failed=$((failed+1))
                errors+=("POST-CONVERT RM FAILED: $src (kept $target)")
            fi
        else
            rm -f -- "$target"
            current_partial=""
            failed=$((failed+1))
            errors+=("CONVERT FAILED: $src :: $(first_line "${err:-unknown error}")")
        fi
    else
        draw_progress "$done_count" "$total" "$elapsed" "$eta" "Moving: $base"
        current_partial="$target"
        if err=$(mv -n -- "$src" "$target" 2>&1); then
            current_partial=""
            moved=$((moved+1))
            done_bytes=$((done_bytes + sz))
        else
            # If mv is cross-fs and was interrupted, a partial may exist at target.
            rm -f -- "$target" 2>/dev/null
            current_partial=""
            failed=$((failed+1))
            errors+=("MOVE FAILED: $src :: $(first_line "${err:-unknown error}")")
        fi
    fi
    done_count=$((done_count+1))
done

elapsed=$SECONDS
draw_progress "$total" "$total" "$elapsed" 0 "Done"
printf '\n\n'
printf 'Summary: %d converted, %d moved, %d failed in %s\n' \
    "$converted" "$moved" "$failed" "$(fmt_time "$elapsed")"
if (( failed > 0 )); then
    printf '\nFailures:\n'
    printf '  %s\n' "${errors[@]}"
    exit 1
fi
