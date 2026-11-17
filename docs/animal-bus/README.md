# Animal Bus Math

A touch game that teaches addition (sums up to 20) to a 3½-year-old, played on an iPad in Safari.

- **Game:** `math.html` at the repo root. One self-contained file: no build step, no dependencies, no network calls.
- **Served at:** `/math` (route in `app.py`).
- **Branch:** `claude/confident-keller-ndlq9v` (not yet merged; no pull request opened).
- **Preview copy:** published as a private Claude artifact, https://claude.ai/artifact/SqPek6AeaqHZYMGp2CNki7. It is a copy of `math.html` without the `<html>/<head>/<body>` wrapper and does not update itself when `math.html` changes.

## Run it

```bash
# simplest: open the file directly
open math.html                     # macOS

# or through the Flask app
pip install -r requirements.txt
python app.py                      # then http://localhost:5000/math
```

On the iPad, open the `/math` URL in Safari and use "Add to Home Screen" for full screen.

## Design rules

These came from the original brief and the research; keep them when changing the game.

1. **At least 80% of play time is the math itself.** The fun has to be the math action. The only non-math parts are choosing a destination (a few seconds) and the arrival scene with a sticker.
2. **No reading needed.** Everything is spoken. Targets are big.
3. **Wrong answers are never punished.** The game counts together with the child, then points at the right answer.
4. **Quantities are always visible as a ten-frame.** Seats are 2 rows of 5 and fill top row first, so 7 looks like "5 and 2" and 13 looks like "a full bus and 3".
5. **The two addends keep their own colour** (orange and blue) from the equation, through the groups, to the seats.

## Three games, same sums

Each trip (round of five sums) plays one game. The games take turns, also across sessions (`state.last`), so two trips in a row are never the same game. All three use the same sums, difficulty steps, ten-seat layout, colours and "count together" help; only the action changes.

| Game (`mode`) | What the child does | Equation shown |
|---|---|---|
| All aboard (`join`) | Puts two groups on the vehicle, then says how many altogether. | `3 + 2 = ?` |
| Fill the seats (`fill`) | The first group is already seated and the seats still needed are outlined in blue. The child taps one animal on at a time until it is full, then says how many got on. | `3 + ? = 5` |
| Who fits? (`fit`) | Same outlined seats, but three groups wait at the stop. The child drags the one that fills the seats exactly. | `3 + ? = 5` |

In "Who fits?" a wrong group is not marked wrong: it gets on, the game says what is left over ("Too many! No seat for two frogs." or "Not full yet! One empty seat."), everyone hops back off, that group fades and the right one pulses. In "Fill the seats" a wrong number is handled like the first game, counting only the animals that just got on.

The grown-up panel can switch any game off.

## How a sum plays (All aboard)

1. Two groups of animal tiles appear with the equation `3 + 2 = ?`. Voice: "three dogs and two frogs".
2. The child drags or taps each group onto the vehicle. Animals hop into seats one at a time.
3. Three answer buttons appear (the numeral plus small dots). Voice: "How many altogether?"
4. The child can tap seated animals to count them one by one.
5. Wrong: "Let's count together", every animal gets a numbered badge, the right button pulses.
6. Right: praise, "three and two make five", an animal noise, and the vehicle drives off.

Five sums make a round. A round is a trip to a place the child picks; a small vehicle at the top moves one stop per sum.

## Difficulty

`STEPS` in `math.html` has 8 steps: 4 levels, each first "counting together" (the game counts aloud during boarding) and then "on their own" (no counting).

| Level | Sums | Vehicles shown |
|---|---|---|
| 1 | within 5 | 1 |
| 2 | 6 to 10 | 1 |
| 3 | pairs that make 10, and 10 + n | 2 |
| 4 | 11 to 20, each addend at most 10 | 2 |

- Levels above 1 mix in about 20% review from earlier levels (`makeQuestion`).
- **Up a step:** at least 4 of the last 5 sums right first try, including the latest (`record`).
- **Down a step:** 3 misses in the last 5.
- **Peekaboo:** in "All aboard" only, in "on their own" steps, about 40% of sums (never the first of a round) hide the animals behind a blind. Tapping the vehicle reveals them.
- The game starts at step 0 because the child counts to 10 reliably but has not started adding.

## Grown-up panel

Press and hold the gear on the home screen for about a second. It sets the step, chooses which of the three games are played, toggles answer-button dots and the voice, and resets stickers (two taps).

## Code map (`math.html`)

Everything is in one IIFE at the bottom of the file.

| Area | Names |
|---|---|
| Content | `WORDS`, `ANIMALS` (emoji, singular, plural, noise), `PRAISE`, `THEMES`, `STEPS`, `ROUND`, `GAMES` |
| Saved state | `state`, `save()`; localStorage key `animalBusMath.v1` holds `step`, `hist`, `stickers`, `dots`, `voice`, `games` (which are ticked), `last` (game played last) |
| Sound | `tone()`, `sfx` (Web Audio beeps, no audio files) |
| Voice | `say(text)` returns a promise; uses the Web Speech API with a timeout fallback because iOS sometimes drops the end event |
| Questions | `makeQuestion(level)`, `makeChoices(sum)` |
| Round flow | `startRound()` (destination choice) → `beginTrip(theme)` (also picks `mode`) → `nextQuestion()` → the game's own action → `win()` → `party()` |
| Game actions | `join`: `board(group)` → `askAnswer()` → `choose()`. `fill`: `presetSeats()`, `buildFeeder()`, `feed()` per tap → `askAnswer()` → `choose()`. `fit`: `presetSeats()`, `buildFitGroups()`, `tryFit(group)` |
| Interaction | `enableDrag()` (pointer events; a tap also boards), `tapCount()`, `peek()` |
| Adaptation | `record(firstTryCorrect)` |
| Scenery | `applyTheme()` sets CSS variables on `body`; `addFloaters()`, `renderTrip()` |

Things that are easy to break:

- `run` is a counter that invalidates in-flight async sequences. Every `await` in the round flow is followed by `if (!alive(id)) return;` so going home mid-sum does not leave a stale sequence running.
- `busy` blocks input while animals are moving or the game is counting. Set it back to `false` on every exit path.
- `q.ask` is the number the child has to find: the total in `join`, the second number in `fill` and `fit`. Answer buttons and the count-together help use `q.ask` and `countable()`, not `q.sum`.
- `--t` (tile size) is rescaled on `#stop` in `fit` so three groups always fit side by side, and on the `fill` feeder. `--t0` is the unscaled size.
- `html`, `body` and `.screen` use `overflow: clip`. With plain `hidden`, tapping an answer button while it was still sliding in from below the edge made the browser scroll the screen up, and the child cannot scroll back.

To add a game, append to `GAMES`, build its pieces in `nextQuestion()`, and end its action with `win(id)`.

To add a destination, append to `THEMES` (`dest`, `place`, `veh`, `word`, optional `flip`, `nowheels`, `float`, and `css` overrides of `BASE`). To add an animal, append to `ANIMALS`; it also becomes a sticker.

## Testing

`docs/animal-bus/playthrough.js` plays full rounds of each of the three games at steps 1, 4, 6 and 8 in landscape and portrait iPad sizes (24 rounds), with one deliberate wrong try per round. It reports page errors, anything that does not fit on the screen, and whether the games take turns, and exits with code 1 if it finds a problem. It needs Playwright.

```bash
npm i -D playwright && npx playwright install chromium
node docs/animal-bus/playthrough.js     # screenshots go to /tmp/animal-bus-*.png
# optional: OUT=<screenshot folder>  CHROME=<path to a Chromium binary>
```

Adding `?fast` to the URL shortens all waits and turns the voice off; the test uses it. `window.__game` exposes `state`, `q`, `mode`, `qn` (sums started), `phase`, `busy` and `theme` for tests.

**Not yet verified on a real iPad:** the spoken voice (it may be blocked inside the Claude artifact viewer) how dragging feels with a small finger, and whether a 3½-year-old follows the two newer games without help. The Claude artifact preview has not been updated with the three games. The playthrough test does not cover the peekaboo path; that was checked once by hand-forcing it.

## Research behind the design

- **Numberblocks, DragonBox Numbers:** a number is shown as its blocks, and adding is two groups physically joining.
- **Early-math sequence:** recognising small amounts at a glance, then ten-frames, then number pairs. Advice for ages 3 to 4 comes mostly from teachers, not controlled studies.
- **Hirsh-Pasek et al. (2015) and Meyer et al. (2021):** sounds, movement and mini-games unrelated to the learning goal pull children off task. This is the basis for rule 1.
- Sums up to 20 are usually age 5 to 6 material, so expect a long stay on levels 1 and 2.

## History

1. Researched popular apps and proposed four designs; chose block groups plus a ten-seat bus, animal theme.
2. First version: board, count, answer, 8 adaptive steps, sticker reward.
3. Feedback "works, but boring": added chosen trips with themed scenery, named animals with noises, peekaboo sums, a journey track, and more sound and movement.
4. Feedback "is it just one game?": added "Fill the seats" and "Who fits?", which practise the same sums as a missing number, and made trips take turns between the three games.

## Ideas not built

- Recorded human voice instead of the device's synthetic one.
- Subtraction: animals get off at a stop.
- A per-sum history for the grown-up panel, to show which pairs are still shaky.
- Publishing the game through GitHub Pages or the existing host so it has a stable public URL.
