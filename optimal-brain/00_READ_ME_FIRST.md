# OPTIMAL BRAIN: TRAINING KNOWLEDGE PACK (READ ME FIRST)

Built 11 October 2026 for Optimal Health and Fitness. This pack gives the Optimal brain (and the individual programme designer in the staff app) the knowledge a master coach with 20+ years of experience would use to write programmes for Optimal members: adults mostly 40+, former athletes, parents getting back into it, people managing menopause and joint issues.

## The files

| File | What it is | When the AI uses it |
|---|---|---|
| 01_Coach_Principles.md | How the best coaches think: Joe DeFranco, Alwyn Cosgrove, Paul Chek, Ben Patrick (Knees Over Toes), Charles Poliquin, Squat University (Dr Aaron Horschig), Vern Gambetta, plus Mike Boyle, Dan John, Stuart McGill, Gray Cook, Peter Attia, Stacy Sims and the LIFTMOR bone study. Ends with the Optimal 40+ filter and a "which coach for which problem" table. | To understand WHY a choice is right and to explain it to coaches. |
| 02_Programming_Framework.md | The rulebook: decision order, member info needed, readiness traffic light, assessment and G1 to G4 grading, movement pattern checklist, session structures, warm up system, loading tables, progression models, regression and pain decision tree, 12 week periodisation, conditioning, special populations (knee, back, shoulder, hip, replacements, osteoporosis, menopause, blood pressure, diabetes, pregnancy, arthritis, former athletes), templates (28 day kickstart, Teams, Groups, Golf, Seniors, fat loss, muscle) and the programme output format. | Every time it builds or edits a programme. |
| 03_Exercise_Library.md | Every exercise in Optimal's 19 category movement library (1,031 unique after merging duplicates) plus 38 signature exercises from the coach research: 1,069 exercises, each with the same fields (grade, why, cues, faults, named progressions and regressions, same level swaps, cautions, equipment, session fit, group flag, prescription, sources). Each category opens with a G1 to G4 ladder. | To choose, progress and regress exercises. |
| Optimal_Exercise_Library.json | The same library as structured data (ids, grades, flags, progression links). | For the staff app / Claude Code workout builder to read directly. |
| Optimal_Exercise_Library.csv | The same library as a spreadsheet (one row per exercise). | For coaches to filter and sort, or import into Sheets. |

## Order of authority (if anything conflicts)
1. Optimal's own rules already in the brain (equipment list, group session rules, BURN/STRONG/HYBRID structure, no jumping in groups, G1 to G4 grading, Ed's programming preferences).
2. File 02 Programming Framework.
3. File 03 Exercise Library.
4. File 01 Coach Principles.

## Key rules in one place
- Safety first: red flags get referred (File 02 Section 12.1); pain above 3/10 means regress (Section 9).
- Groups: no jumping or high impact of any kind. Every movement has G1 to G4 options. Only use exercises marked Group: Yes in File 03.
- Teams: individual programmes, can use G4 and screened impact work.
- Every week covers squat, hinge, single leg, horizontal and vertical push and pull, anti extension, anti rotation, anti lateral flexion, carries, power, balance, conditioning; about 2 pulls per push for desk workers.
- Every exercise in a programme gets: sets, reps, tempo, rest, RPE, one cue, one progression rule and at least one regression.
- 4 week blocks to match the 28 day rhythm; retest at the end of each block.

## Notes for the team
- The coach summaries are written in our own words from the coaches' published work and research; they are a working coaching summary, not a quote of any book.
- Kit marked "confirm" in the library (for example sliders, Swiss ball, battle ropes, treadmill, foam roller, tib bar) is not on the confirmed equipment list; the brain's equipment rules decide whether it can be programmed.
- Grades in the library were set by rule (load, support, complexity) and should be sanity checked by the coaches as they use them; correct any that feel off and the brain will follow the corrected version.
