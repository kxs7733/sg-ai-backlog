// Plays full rounds of all three games at steps 1, 4, 6 and 8, in landscape and portrait iPad sizes,
// with one deliberate wrong try per round, and reports page errors.
//   node docs/animal-bus/playthrough.js
// Optional: OUT=<folder for screenshots, default /tmp>  CHROME=<path to a Chromium binary>
const { chromium } = require('playwright');
const path = require('path');
const OUT = process.env.OUT || '/tmp';
const GAMES = ['join', 'fill', 'fit'];

(async () => {
  const b = await chromium.launch(process.env.CHROME ? { executablePath: process.env.CHROME } : {});
  const errs = [];
  const shot = (p, name) => p.screenshot({ path: path.join(OUT, `animal-bus-${name}.png`) });
  const tap = async loc => {
    await loc.dispatchEvent('pointerdown', { pointerId: 5, clientX: 1, clientY: 1 });
    await loc.dispatchEvent('pointerup', { pointerId: 5, clientX: 1, clientY: 1 });
  };
  const idle = p => p.waitForFunction(() => !__game.busy);
  const seatedCount = p => p.locator('.seat .tile').count();

  for (const [name, vp] of [['land', { width: 1024, height: 700 }], ['port', { width: 768, height: 950 }]]) {
    const p = await b.newPage({ viewport: vp, hasTouch: true });
    p.on('pageerror', e => errs.push(String(e)));
    p.on('console', m => m.type() === 'error' && errs.push(m.text()));
    await p.goto('file://' + path.resolve(__dirname, '../../math.html') + '?fast');
    await shot(p, `${name}-home`);

    for (const game of GAMES) {
      for (const step of [0, 3, 5, 7]) {
        const tag = `${name}-${game}-s${step}`;
        await p.evaluate(([s, g]) => {
          __game.state.step = s; __game.state.hist = []; __game.state.last = '';
          for (const k in __game.state.games) __game.state.games[k] = k === g;
        }, [step, game]);
        await p.click('#play'); await p.locator('.dest-btn').first().click({ force: true });
        for (let i = 0; i < 5; i++) {
          await p.waitForFunction(() => __game.phase === 'board' && !__game.busy);
          const { q, mode, qn } = await p.evaluate(() => ({ q: __game.q, mode: __game.mode, qn: __game.qn }));
          if (mode !== game) errs.push(`${tag}: playing ${mode}, expected ${game}`);
          if (i === 0) { await p.waitForTimeout(700); await shot(p, `${tag}-board`); }
          // nothing may stick out of the screen
          // (layout position, so the slide-in and wiggle animations do not count)
          const wide = await p.evaluate(() => [...document.querySelectorAll('#stop .group, #road .bus')].some(e => {
            let x = 0, y = 0;
            for (let n = e; n; n = n.offsetParent) { x += n.offsetLeft; y += n.offsetTop; }
            return x < 0 || x + e.offsetWidth > innerWidth || y + e.offsetHeight > innerHeight;
          }));
          if (wide) errs.push(`${tag}: ${q.a}+${q.b} does not fit the screen`);

          if (game === 'join') {
            // drag the first group, tap the second
            const bb = await p.locator('.group').first().boundingBox(), rb = await p.locator('#road').boundingBox();
            await p.mouse.move(bb.x + bb.width / 2, bb.y + bb.height / 2); await p.mouse.down();
            await p.mouse.move(rb.x + rb.width / 2, rb.y + rb.height / 2, { steps: 5 }); await p.mouse.up();
            await idle(p);
            await tap(p.locator('.group.gb'));
          } else if (game === 'fill') {
            if (await seatedCount(p) !== q.a) errs.push(`${tag}: first group not seated`);
            for (let k = 0; k < q.b; k++) {
              await p.locator('.feeder').dispatchEvent('pointerdown', { pointerId: 5, clientX: 1, clientY: 1 });
              await p.waitForFunction(() => !__game.busy || __game.phase === 'answer');
            }
          } else {
            if (i === 0) { // a group that does not fit: everyone must hop back off
              const wrong = p.locator(`#stop .group:not([data-n="${q.b}"])`).first();
              await tap(wrong); await idle(p);
              if (await seatedCount(p) !== q.a) errs.push(`${tag}: wrong group stayed on`);
              if (!(await wrong.evaluate(e => e.classList.contains('tried')))) errs.push(`${tag}: wrong group not marked`);
              await shot(p, `${tag}-wrong`);
            }
            await tap(p.locator(`#stop .group[data-n="${q.b}"]`));
          }

          if (game !== 'fit') {
            await p.waitForFunction(() => __game.phase === 'answer' && !__game.busy);
            const n = await seatedCount(p);
            if (n !== q.sum) errs.push(`${tag}: seated ${n} != ${q.sum}`);
            if (i === 0) { // wrong answer first: the game counts together
              await p.locator(`.choice:not([data-n="${q.ask}"])`).first().click({ force: true });
              await idle(p);
              await shot(p, `${tag}-wrong`);
            }
            await p.click(`.choice[data-n="${q.ask}"]`, { force: true });
          }
          await p.waitForFunction(n => __game.qn > n || __game.phase === 'party', qn);
        }
        await p.waitForFunction(() => __game.phase === 'party');
        console.log(name, game, 'step', step, '-> now', await p.evaluate(() => __game.state.step), 'stickers', await p.evaluate(() => __game.state.stickers.length));
        if (step === 0 && game === 'join') await shot(p, `${name}-party`);
        await p.click('#partyHome');
      }
    }

    // with every game ticked, trips take turns
    await p.evaluate(() => { __game.state.last = ''; for (const k in __game.state.games) __game.state.games[k] = true; });
    const order = [];
    for (let i = 0; i < 4; i++) {
      await p.click('#play'); await p.locator('.dest-btn').first().click({ force: true });
      order.push(await p.evaluate(() => __game.mode));
      await p.click('#homeBtn');
    }
    if (order.join() !== 'join,fill,fit,join') errs.push(`${name}: games did not take turns: ${order}`);
    await p.close();
  }
  console.log('errors:', errs);
  await b.close();
  process.exit(errs.length ? 1 : 0);
})();
