"""Export the S8 review's independent HTTP comparisons (requires Matplotlib)."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.lines import Line2D


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--font', type=Path, help='Korean-capable font file; defaults to macOS fonts')
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    data = json.loads(args.input.read_text(encoding='utf-8'))
    candidates = [str(args.font)] if args.font else ['/System/Library/Fonts/Supplemental/AppleGothic.ttf', '/System/Library/Fonts/AppleSDGothicNeo.ttc']
    for candidate in candidates:
        if Path(candidate).exists():
            font_manager.fontManager.addfont(candidate)
            plt.rcParams['font.family'] = font_manager.FontProperties(fname=candidate).get_name()
            break
    else:
        parser.error('A Korean font is required; provide --font /path/to/font.ttf')
    plt.rcParams['axes.unicode_minus'] = False
    labels = {
        'consumers': ('Consumer/활성 상한', ['1/1', '2/2', '3/3']),
        'partitions': ('캠페인 요청·Retry 파티션', ['3', '6']),
        'compression': ('캠페인 압축', ['none', 'LZ4']),
        'window': ('캠페인 Relay 제출 창', ['1', '10']),
    }
    fig, axes = plt.subplots(2, 2, figsize=(12, 8.5), dpi=180)
    for axis, study in zip(axes.flat, data['studies']):
        title, ticks = labels[study['id']]
        maximum = max(float(r['ratePerSecond']) for r in study['runs'])
        for x, group in enumerate(study['byCondition']):
            rows = [r for r in study['runs'] if r['condition'] == group['condition']]
            axis.scatter([x - .07, x + .07], [float(r['ratePerSecond']) for r in rows],
                         color='#2878B5', s=40, zorder=3)
            rate = float(group['pooledRatePerSecond'])
            axis.scatter([x], [rate], color='#20242C', marker='D', s=32, zorder=4)
            axis.annotate(f'{rate:.3f}', (x, max(float(r['ratePerSecond']) for r in rows)),
                          xytext=(0, 10), textcoords='offset points', ha='center', fontsize=10)
        axis.set_title(f"{title} · 기준 {study['baseCommit'][:7]}", fontsize=12)
        axis.set_xticks(range(len(ticks)), ticks)
        axis.set_xlim(-.5, len(ticks)-.5)
        axis.set_ylim(0, maximum * 1.22)
        axis.set_ylabel('캠페인 건/초')
        axis.grid(axis='y', alpha=.2)
        axis.spines[['top', 'right']].set_visible(False)
    fig.suptitle('S8 HTTP Mock 실험별 반복 관측', fontsize=18, y=.98)
    fig.text(.5, .935, '각 칸 안에서만 비교 · 실험 사이의 코드·조건 차이를 합쳐 순위나 개선율을 만들지 않음', ha='center', fontsize=11)
    fig.legend(handles=[Line2D([], [], marker='o', color='#2878B5', linestyle='', label='개별 실행 (설정별 2회)'),
                        Line2D([], [], marker='D', color='#20242C', linestyle='', label='총 건수 ÷ 총 발송 시간 (숫자 라벨)')],
               loc='upper center', bbox_to_anchor=(.5,.91), ncol=2, frameon=False)
    fig.text(.5,.025,'각 실행 캠페인 10,000건 · 서로 다른 y축 범위 · 점의 차이는 관측 변동이며 신뢰구간이 아님 · 단말 도착 측정 아님',ha='center',fontsize=10)
    fig.tight_layout(rect=[.02,.06,.98,.86], h_pad=2.5, w_pad=2)
    fig.savefig(args.output, facecolor='white', metadata={'Description':'S8 review: per-run rates and pooled rates within independent studies'})
    plt.close(fig)


if __name__ == '__main__':
    main()
