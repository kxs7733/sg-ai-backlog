// Plays full rounds of every mini-game at every level, in landscape and portrait iPad sizes, with one deliberate
// wrong answer per round, and reports page errors and anything that does not fit on the screen.
//   node docs/animal-bus/playthrough.js
// Optional: OUT=<folder for screenshots, default /tmp>  CHROME=<path to a Chromium binary>
const { chromium } = require('playwright');
const path = require('path');
const OUT = process.env.OUT || '/tmp';
const GAMES = ['bus', 'bush', 'frog', 'dice', 'pop'];
const ROUND = 8;

(async () => {
  const b = await chromium.launch(process.env.CHROME ? { executablePath: process.env.CHROME } : {});
  const errs = [];
  const shot = (p, name) => p.screenshot({ path: path.join(OUT, `animal-math-${name}.png`) });
  const down = loc => loc.dispatchEvent('pointerdown', { pointerId: 5, clientX: 1, clientY: 1 });
  const asking = (p, n) => p.waitForFunction(n => __game.phase === 'ask' && !__game.busy && __game.qn > n, n);
  const info = p => p.evaluate(() => ({ q: __game.q, game: __game.game, qn: __game.qn }));
  const only = (p, games, level) => p.evaluate(([g, lv]) => {
    __game.state.level = lv; __game.state.hist = []; __game.state.last = '';
    for (const k in __game.state.games) __game.state.games[k] = g.includes(k);
  }, [games, level]);

  for (const [name, vp] of [['land', { width: 1024, height: 700 }], ['port', { width: 768, height: 950 }]]) {
    const p = await b.newPage({ viewport: vp, hasTouch: true });
    p.on('pageerror', e => errs.push(String(e)));
    p.on('console', m => m.type() === 'error' && errs.push(m.text()));
    await p.goto('file://' + path.resolve(__dirname, '../../math.html') + '?fast');
    await shot(p, `${name}-home-empty`);

    for (const want of GAMES) {
      for (const level of [0, 1, 2, 3]) {
        const tag = `${name}-${want}-L${level + 1}`;
        await only(p, [want], level);
        const before = await p.evaluate(() => __game.state.caught.length);
        let qn = (await info(p)).qn;
        await p.click('#play', { force: true });
        const played = new Set();
        for (let i = 0; i < ROUND; i++) {
          await asking(p, qn);
          const cur = await info(p); qn = cur.qn; const { q } = cur; played.add(cur.game);
          if (await p.locator(`.opt[data-n="${q.sum}"]`).count() !== 1) errs.push(`${tag}: ${q.a}+${q.b} has no single right answer on screen`);
          if (i === 0) { await p.waitForTimeout(600); if (cur.game === want) await shot(p, `${tag}`); }
          // nothing may stick out of the screen (layout position, so animations do not count)
          const wide = await p.evaluate(() => [...document.querySelectorAll('#stage > *, .opt')].some(e => {
            let x = 0, y = 0;
            for (let n = e; n; n = n.offsetParent) { x += n.offsetLeft; y += n.offsetTop; }
            return x < 0 || x + e.offsetWidth > innerWidth || y < 0 || y + e.offsetHeight > innerHeight;
          }));
          if (wide) errs.push(`${tag}: ${q.a}+${q.b} in ${cur.game} does not fit the screen`);
          if (i === 0) { // wrong answer first: the game counts together, then he can still answer
            await down(p.locator(`.opt:not([data-n="${q.sum}"])`).first());
            await p.waitForFunction(() => __game.phase === 'ask' && !__game.busy);
            // the miss must be visible: the wrong answer is marked and the count-together help has run
            const helped = await p.evaluate(() => !!document.querySelector('.opt.wrong') && (document.querySelectorAll('.badge').length > 0 || !!document.querySelector('.pad.pulse')));
            if (!helped) errs.push(`${tag}: wrong answer in ${cur.game} did not show the help`);
            if (cur.game === want) await shot(p, `${tag}-wrong`);
          }
          await down(p.locator(`.opt[data-n="${q.sum}"]`));
          await p.waitForFunction(n => __game.qn > n || __game.phase === 'party', qn);
        }
        await p.waitForFunction(() => __game.phase === 'party');
        const got = await p.evaluate(() => __game.state.caught.length) - before;
        if (got !== ROUND) errs.push(`${tag}: caught ${got} creatures, expected ${ROUND}`);
        console.log(name, want, 'level', level + 1, 'played', [...played].join('+'), '-> level now', await p.evaluate(() => __game.state.level + 1));
        if (want === 'bus' && level === 0) await shot(p, `${name}-party`);
        await p.click('#partyHome', { force: true });
      }
    }
    await shot(p, `${name}-home-full`);

    // with every game on: several different games in one round, and never the same one twice in a row
    await only(p, GAMES, 0);
    let qn = (await info(p)).qn; const seq = [];
    await p.click('#play', { force: true });
    for (let i = 0; i < ROUND; i++) {
      await asking(p, qn);
      const cur = await info(p); qn = cur.qn; seq.push(cur.game);
      if (i === 1) { // tap one thing to count it, then answer: recorded as "counted himself"
        await p.evaluate(() => { const t = document.querySelector('#stage .tile, #stage .pip.on'); if (t) t.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true })); });
      }
      if (i === 2) await p.waitForFunction(() => __game.shy);   // answer while the golden creature is out
      await down(p.locator(`.opt[data-n="${cur.q.sum}"]`));
      await p.waitForFunction(n => __game.qn > n || __game.phase === 'party', qn);
    }
    await p.waitForFunction(() => __game.phase === 'party');
    if (new Set(seq).size < 3) errs.push(`${name}: only ${new Set(seq).size} different games in a round: ${seq}`);
    if (seq.some((g, i) => i && g === seq[i - 1])) errs.push(`${name}: same game twice in a row: ${seq}`);
    const st = await p.evaluate(() => ({ log: __game.state.log.slice(-8), caught: __game.state.caught.slice(-8), egg: __game.state.egg }));
    if (!st.caught.some(c => c.g)) errs.push(`${name}: quick answer did not catch a golden creature`);
    if (!st.log.some(x => x.r === 'c') && seq[1] !== 'frog') errs.push(`${name}: counting by himself was not recorded`);
    if (!st.log.some(x => x.r === 's')) errs.push(`${name}: no straight-away answers recorded`);
    if (!st.egg) errs.push(`${name}: no egg after a round`);
    console.log(name, 'mixed round:', seq.join(' '));
    await p.click('#partyHome', { force: true });

    // an egg from an earlier day hatches into a creature
    const n0 = await p.evaluate(() => { __game.state.egg = 'Mon Jan 01 2024'; __game.renderHome(); return __game.state.caught.length; });
    await down(p.locator('#egg'));
    if (await p.evaluate(() => __game.state.caught.length) !== n0 + 1) errs.push(`${name}: egg did not hatch`);
    await p.close();
  }
  console.log('errors:', errs);
  await b.close();
  process.exit(errs.length ? 1 : 0);
})();
