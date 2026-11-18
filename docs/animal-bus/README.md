# Animal Math

A touch game that teaches addition to a 3½-year-old, played on an iPad in Safari. It started as "Animal Bus Math"; the folder keeps that name.

**Play:** https://web-production-0c746394.up.railway.app (in Safari, use "Add to Home Screen" for full screen). The address is public: anyone who has it can play, but it is not listed anywhere. Creatures and progress are saved on the device, not on the server.

## Where things are

Everything is in this folder, `~/Documents/animal-bus-math`:

| File | What it is |
|---|---|
| `math.html` | The game. One self-contained file: no build step, no images or sound files, no network calls. |
| `README.md` | This document. |
| `playthrough.js` | Automated test that plays every mini-game at every level. |
| `app.py`, `requirements.txt`, `Procfile` | The small server Railway uses to put the game online. |

Other copies:

- **GitHub backup:** `kxs7733/sg-ai-backlog`, branch `claude/confident-keller-ndlq9v` (`math.html` at the root, this README and the test under `docs/animal-bus/`). Not merged, no pull request. This folder is not a git repository; the backup is updated by copying the files across.
- **Old preview:** a private Claude artifact, https://claude.ai/artifact/SqPek6AeaqHZYMGp2CNki7. It shows the very first version and does not update.

## Design rules

Version 3 (18 Nov 2026) was written after a real play test: the child was bored within five minutes, and an independent review agreed with the parent's criticisms. These rules come from that.

1. **Forward sums only:** `a + b = ?`. Missing-number sums (`1 + ? = 2`) were too hard and are gone.
2. **He can answer from the first moment.** Nothing has to be loaded, dragged or counted first. If he knows 1 + 1, one tap.
3. **The game never counts aloud before he answers.** Earlier versions counted as pieces loaded, so he could repeat the last number he heard. Now he may tap things to count them himself (the game says only the number he is on), and the game counts together with him only after a miss.
4. **Same sum, different game.** Five mini-games show a sum in five different ways. The game changes on every sum and never repeats twice in a row.
5. **Every right answer catches a creature.** A quick right answer catches a golden one. There is no clock on screen and nothing is lost by being slow.
6. **Wrong answers are never punished.** The wrong choice fades, the game counts together, and he answers again.
7. **No reading needed.** Everything is spoken; targets are big.
8. **The two numbers keep their colours** (orange and blue) in the equation and in every game.

## The five mini-games

Each is one sum. `pickGame()` chooses among the games that suit the numbers and are ticked in the grown-up panel.

| Game (`id`) | How the sum is shown | How he answers | Used when |
|---|---|---|---|
| Bus (`bus`) | Both groups sit in ten seats, two rows of five, so 7 looks like "5 and 2". | Number buttons | Any sum |
| Hiding bush (`bush`) | Both groups are shown, then hop into a bush and vanish. He has to hold the numbers in his head. Tapping the bush lets them out again. | Number buttons | Sums up to 10 |
| Frog hop (`frog`) | A number line of lily pads. The frog sits on the first number; blue dots show how many hops. He can tap the frog to hop it one pad at a time. | Taps the pad it lands on | Sums up to 10 |
| Dice monster (`dice`) | Two dice, with patterns he can recognise at a glance. | Feeds the monster the right numbered cookie | Both numbers up to 6 |
| Balloons (`pop`) | Two loose groups of animals. | Pops the right numbered balloon | Any sum |

The three choices are picked to show how he is thinking: the right total, one of the two numbers on screen (he did not add), and a near miss (`makeChoices()`).

## Rewards

- **Creatures.** Each is built from parts (body shape, colour, eyes, hat), so every catch is new. No existing characters are used.
- **Golden creature.** After each question a golden creature peeks in from the right for about nine seconds (`SHY`). A right first answer while it is out catches a golden one.
- **Round.** Eight sums (`SESSION`). The row of circles at the top fills with the creatures caught.
- **Meadow.** The home screen shows the last 28 creatures wandering about. Tapping one makes it jump and squeak.
- **Egg.** A round ends with an egg, unless one is already waiting. It hatches on a later day into a golden creature with a crown, to bring him back.

## Difficulty

| Level | Sums |
|---|---|
| 1 | within 5 |
| 2 | 6 to 10 |
| 3 | pairs that make 10, and 10 + n |
| 4 | 11 to 20, each number at most 10 |

- Levels above 1 mix in about 20% review from earlier levels.
- **Up a level:** 11 of the last 12 sums right on the first try (about two rounds). **Down:** 4 misses in the last 12. Counting things himself before answering still counts as right.
- Sums up to 20 are usually age 5 to 6 material, so expect a long stay on levels 1 and 2.

## Grown-up panel

Press and hold the gear on the home screen for about a second.

- **Is he understanding or guessing?** The top of the panel shows, for the last 30 sums: how many he answered straight away, how many after counting himself, how many he missed first, how many times he picked one of the two numbers on screen instead of adding, and which sums he missed.
- Set the level, tick which mini-games appear, turn the voice on or off, choose which voice speaks, reset everything (two taps).

## Sound and voice

- **iPad speaker.** In silent mode an iPad plays web-page sound through earphones but not the speaker. On the first touch the game declares itself as media playback (`unmute()`), which should make it ignore silent mode. Not yet confirmed on the iPad. If it is still silent, turn off Silent Mode in Control Centre (the bell).
- **Voice.** The game speaks with the device's built-in voices. It prefers the voice chosen in the grown-up panel, then any downloaded Premium or Enhanced English voice. For a better voice, download one in the iPad's Settings (Accessibility, then Spoken Content or Read & Speak, then Voices, English) and pick it in the panel. Whether Safari offers every downloaded voice to web pages is not confirmed.
- A recorded or AI-generated natural voice would sound much better. Not built: it needs a text-to-speech service and a set of audio clips.

## Hosting (Railway)

- Railway project `animal-bus-math`, service `web`, in the workspace "kxs7733's Projects". `app.py` serves `math.html` at `/`; nothing else in this folder is served.
- There is no GitHub repository behind it. A change goes live only when you redeploy:

```bash
cd ~/Documents/animal-bus-math
railway up --service web
```

## Code map (`math.html`)

Everything is in one function at the bottom of the file.

| Area | Names |
|---|---|
| Content | `WORDS`, `ANIMALS`, `PRAISE`, `HATS`, `LEVELS`, `SESSION`, `SHY` |
| Saved state | `state`, `save()`; localStorage key `animalMath.v2` holds `level`, `hist`, `caught`, `log`, `egg`, `voice`, `voiceName`, `games`, `last` |
| Sound and voice | `unlockAudio()`, `unmute()`, `tone()`, `sfx`, `say(text)` (always resolves; iOS sometimes drops the end event), `pickVoice()` |
| Creatures | `newCreature()`, `mk()` (builds the element), the `.cr` styles |
| Questions | `makeQuestion()`, `makeChoices()` |
| Engine | `next()` → the game's `setup()` → `answer(n, node)` → the game's `reveal()` → `countAll()` → `catchCreature()` → `next()` or `party()` |
| Games | The `GAMES` array. Each entry has `id`, `name`, `ok()` (does it suit these numbers), `setup(stage)`, `ask()` (what is spoken), `items()` (things to count), `reveal()` (after a right answer), `teach()` (after a miss), and optionally `intro()` and `after()` |
| Tracking | `record()` writes one line per sum to `state.log` (`s` straight away, `c` counted himself, `m` missed first) and moves the level; `stats()` shows it |
| Home | `renderHome()` (meadow, egg), `startRound()`, `goHome()` |

Things that are easy to break:

- `run` is a counter that invalidates in-flight sequences. Every `await` in the engine and the games is followed by an `alive(id)` check, so going home mid-sum does not leave a stale sequence running.
- `busy` blocks input while the game is revealing or counting. Set it back to `false` on every exit path.
- Every tappable answer has class `opt` and `data-n`. The equation's answer uses class `ans`; do not mix them (the test looks for `.opt`).
- Use `calc(var(--t) * n)` for padding inside game pieces, never a percentage: percentages are measured against the screen width and once squeezed the dice dots to nothing.
- `html`, `body` and `.screen` use `overflow: clip` so the browser cannot scroll the screen out of place.

To add a mini-game, append an entry to `GAMES` and add its id to `state.games`.

## Testing

`playthrough.js` plays a full round of each mini-game at each of the four levels in landscape and portrait iPad sizes (40 rounds), with one deliberate wrong answer per round. It checks for page errors, anything that does not fit on the screen, that a miss shows the help, that eight creatures are caught per round, that a mixed round uses at least three games and never the same one twice running, that a quick answer catches a golden creature, that counting first is recorded, and that an egg is laid and hatches. It exits with code 1 if it finds a problem. It needs Playwright.

```bash
npm i -D playwright && npx playwright install chromium
cd ~/Documents/animal-bus-math
node playthrough.js                     # screenshots go to /tmp/animal-math-*.png
# optional: OUT=<screenshot folder>  CHROME=<path to a Chromium binary>
```

Adding `?fast` to the URL shortens all waits and turns the voice off; the test uses it. `window.__game` exposes `state`, `q`, `game`, `qn`, `phase`, `busy` and `shy` for tests.

**Not verified on a real child or iPad:** whether version 3 holds his interest, the speaker fix, the voice, and whether he understands each mini-game without help. The test proves the game runs, not that it is fun.

## History

1. Researched popular apps; built one game: load two groups onto a ten-seat bus, count, answer.
2. "Works, but boring": added trips, scenery, animal noises, peekaboo sums.
3. "Is it just one game?": added two missing-number games, first on the same bus, then reskinned as a monster and a house.
4. Real play test, 17 Nov 2026: bored in five minutes; missing-number sums too hard; forced to load every piece before answering; unclear whether he understood or repeated the count. An independent review found the three games were one loop in three skins, that the game gave the answer away by counting aloud, and that a sum took 20 to 25 seconds with input locked for most of it.
5. Version 3, 18 Nov 2026: rewritten around the design rules above.

## Ideas not built

- More mini-games: a baker's tray where he taps out the total himself, a tower to stack, fish to net.
- A natural recorded or AI voice.
- Subtraction.
- Its own GitHub repository, so the folder is backed up and deploys by itself.
