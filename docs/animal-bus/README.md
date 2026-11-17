# Animal Bus Math

A touch game that teaches addition (sums up to 20) to a 3½-year-old, played on an iPad in Safari.

## Where things are

Everything is in this folder, `~/Documents/animal-bus-math`:

| File | What it is |
|---|---|
| `math.html` | The game. One self-contained file: no build step, no dependencies, no network calls. |
| `README.md` | This document: design rules, the three games, difficulty, code map, history. |
| `playthrough.js` | Automated test that plays every game at every level. |
| `app.py`, `requirements.txt`, `Procfile` | The small server Railway uses to put the game online. |

Other copies:

- **GitHub:** `kxs7733/sg-ai-backlog`, branch `claude/confident-keller-ndlq9v` (`math.html` at the root, the other two files under `docs/animal-bus/`). The first sessions were pushed there; it is not merged and has no pull request. This folder was copied from that branch on 17 Nov 2026 and is not a git repository, so changes made here are not backed up until they are copied back or the folder gets its own repository.
- **Old preview:** a private Claude artifact, https://claude.ai/artifact/SqPek6AeaqHZYMGp2CNki7. It still shows the first, single-game version and does not update itself.

## Run it

```bash
open ~/Documents/animal-bus-math/math.html
```

On the iPad, open **https://web-production-0c746394.up.railway.app** in Safari and use "Add to Home Screen" for full screen. The address is public: anyone who has it can play, but it is not listed anywhere. Progress and stickers are saved on the device, not on the server.

## Hosting (Railway)

- Railway project `animal-bus-math`, service `web`, in the workspace "kxs7733's Projects". First deployed 17 Nov 2026.
- `app.py` (Flask) serves `math.html` at `/`; `requirements.txt` and `Procfile` tell Railway how to run it. Nothing else in this folder is served.
- There is no GitHub repository behind it. The folder is uploaded straight from this Mac, so a change goes live only when you redeploy:

```bash
cd ~/Documents/animal-bus-math
railway up --service web
```

## Design rules

These came from the original brief and the research; keep them when changing the game.

1. **At least 80% of play time is the math itself.** The fun has to be the math action. The only non-math parts are choosing a destination (a few seconds) and the arrival scene with a sticker.
2. **No reading needed.** Everything is spoken. Targets are big.
3. **Wrong answers are never punished.** The game counts together with the child, then points at the right answer.
4. **Quantities are always visible as a ten-frame.** Seats are 2 rows of 5 and fill top row first, so 7 looks like "5 and 2" and 13 looks like "a full bus and 3".
5. **The two addends keep their own colour** (orange and blue) from the equation, through the groups, to the seats.

## Three games, same sums

The home screen shows one button per game. The child picks; the button whose turn it is wiggles (the game after the one played last, `state.last`), and the play-again button after a round moves on to the next game. A round is five sums. All three use the same sums, difficulty steps, ten-frame layout, orange and blue groups and "count together" help. Each has its own look and story so they do not feel like one game.

| Game (`mode`) | Looks like | What the child does | Equation shown |
|---|---|---|---|
| Animal Bus (`join`) 🚌 | A vehicle on a trip to a place the child picks | Puts two groups of animals on, then says how many altogether. | `3 + 2 = ?` |
| Hungry Monster (`fill`) 👾 | A purple monster whose tummy is the ten-frame | The monster has already eaten some food and the spaces still needed are outlined in blue. The child taps food in one piece at a time until it is full, then says how many it ate. | `3 + ? = 5` |
| Bedtime House (`fit`) 🏠 | A house at night whose beds are the ten-frame | Some animals are in bed and the empty beds are outlined. Three groups wait outside; the child drags the one that fills the beds exactly. | `3 + ? = 5` |

In Bedtime House a wrong group is not marked wrong: it goes in, the game says what is left over ("Too many! No bed for two frogs." or "Not full yet! One empty bed."), everyone hops back out, that group fades and the right one pulses. In Hungry Monster a wrong number is handled like Animal Bus, counting only the food just eaten.

Hungry Monster and Bedtime House ask for a missing number, which is harder than plain adding. The grown-up panel can switch any game off; an unticked game disappears from the home screen.

## Sound and voice

- **iPad speaker.** In silent mode an iPad plays web-page sound through earphones but not the speaker. On the first touch the game declares itself as media playback (`unmute()`: `navigator.audioSession.type = 'playback'` plus a looping silent `<audio>` element), which should make it ignore silent mode. Written on 17 Nov 2026 after the game was silent on the speaker; not yet confirmed on the iPad. If it is still silent, turn off Silent Mode in Control Centre (the bell).
- **Voice.** The game speaks with the device's built-in voices (Web Speech). `pickVoice()` prefers the voice chosen in the grown-up panel, then any downloaded Premium or Enhanced English voice, then the default. Lines ending in "!" are spoken at a higher pitch. For a better voice, download one in the iPad's Settings (Accessibility, then Spoken Content or Read & Speak, then Voices, English) and pick it in the grown-up panel; choosing a voice plays a sample. Whether Safari offers every downloaded voice to web pages is not confirmed.
- A recorded or AI-generated natural voice would sound much better than any built-in one. Not built: it needs a text-to-speech service and a set of audio clips.

## How a sum plays (Animal Bus)

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
- **Peekaboo:** in Animal Bus only, in "on their own" steps, about 40% of sums (never the first of a round) hide the animals behind a blind. Tapping the vehicle reveals them.
- The game starts at step 0 because the child counts to 10 reliably but has not started adding.

## Grown-up panel

Press and hold the gear on the home screen for about a second. It sets the step, chooses which of the three games appear, toggles answer-button dots and the voice, picks which voice speaks, and resets stickers (two taps).

## Code map (`math.html`)

Everything is in one IIFE at the bottom of the file.

| Area | Names |
|---|---|
| Content | `WORDS`, `ANIMALS` (emoji, singular, plural, noise), `PRAISE`, `THEMES`, `STEPS`, `ROUND`, `GAMES` |
| Saved state | `state`, `save()`; localStorage key `animalBusMath.v1` holds `step`, `hist`, `stickers`, `dots`, `voice`, `games` (which are ticked), `last` (game played last) |
| Sound | `tone()`, `sfx` (Web Audio beeps, no audio files) |
| Voice | `say(text)` returns a promise; uses the Web Speech API with a timeout fallback because iOS sometimes drops the end event |
| Questions | `makeQuestion(level)`, `makeChoices(sum)` |
| Home | `renderHome()` (game buttons), `play(id)` sets `mode`, `nextGame()` is whose turn it is; each entry in `GAMES` has an `icon` and, except the bus, its own `theme` |
| Round flow | Bus: `startRound()` (destination choice) → `beginTrip(theme)`. Monster and house: `beginTrip(game.theme)` directly. Then `nextQuestion()` → the game's own action → `win()` → `party()` |
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

`playthrough.js` plays full rounds of each of the three games at steps 1, 4, 6 and 8 in landscape and portrait iPad sizes (24 rounds), with one deliberate wrong try per round. It reports page errors, anything that does not fit on the screen, and whether the games take turns, and exits with code 1 if it finds a problem. It needs Playwright.

```bash
npm i -D playwright && npx playwright install chromium
cd ~/Documents/animal-bus-math
node playthrough.js                     # screenshots go to /tmp/animal-bus-*.png
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
4. Feedback "is it just one game?": added two games that practise the same sums as a missing number. The first attempt put them on the same bus and rotated them automatically, so it still looked like one game. Reworked into three home-screen buttons with their own look: Animal Bus, Hungry Monster, Bedtime House.
5. Feedback "no sound on the speaker, voice is dull": added the silent-mode workaround, a livelier delivery and a voice picker.

## Ideas not built

- Recorded human voice instead of the device's synthetic one.
- Subtraction: animals get off at a stop.
- A per-sum history for the grown-up panel, to show which pairs are still shaky.
- Publishing the game through GitHub Pages or the existing host so it has a stable public URL.
