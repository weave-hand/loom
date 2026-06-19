# Thresholds mirror loom-complexity/render.jq.
# Waivers: triage is intentionally waiver-unaware; the skill layer decides whether to skip.
def t_cc: 15;
def t_cog: 15;
def t_mi: 20;
def t_sloc: 100;
def r2: (.*100|round)/100;
[ .[]
  | (.name | ltrimstr($root)) as $file
  | .. | objects | select(.kind=="function")
  | { file:$file, function:(.name // "<anon>"), line:(.start_line // 0),
      cc:(.metrics.cyclomatic.sum // 0), cog:(.metrics.cognitive.sum // 0),
      mi:(.metrics.mi.mi_visual_studio // 100), sloc:(.metrics.loc.sloc // 0) }
  | . + { value: (
        (if .cc  > t_cc   then .cc  - t_cc          else 0 end)
      + (if .cog > t_cog  then .cog - t_cog          else 0 end)
      + (if .sloc > t_sloc then (.sloc - t_sloc) / 50 else 0 end)
      + (if .mi  < t_mi   then (t_mi - .mi)          else 0 end) ) }
]
| map(select(.value > 0))
| group_by(.file)
| map({ file: .[0].file,
        theme_value: ((map(.value) | add) | r2),
        members: ( sort_by(-.value)
                   | map({ function, line, cc, cog, mi:(.mi|r2), sloc, value:(.value|r2) }) ) })
| sort_by(-.theme_value)
