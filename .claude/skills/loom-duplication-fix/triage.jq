def loc(s): (s.path + ":" + (s.start_line|tostring) + "-" + (s.end_line|tostring));
def is_test(p): (p | test("(^|/)tests?/")) ;
[ .duplicates[]?
  | { hub:.file1.path, partner:.file2.path, lines:.line_count, a:.file1, b:.file2 },
    { hub:.file2.path, partner:.file1.path, lines:.line_count, a:.file2, b:.file1 } ]
| group_by(.hub)
| map({ file: .[0].hub,
        total_lines: (map(.lines) | add),
        pair_count: length,
        partners: (map(.partner) | unique),
        test_only: ( ([ .[0].hub ] + (map(.partner))) | unique | all(is_test(.)) ),
        pairs: ( sort_by(-.lines)
                 | map({ lines, a: loc(.a), b: loc(.b) }) ) })
| sort_by(-.total_lines)
