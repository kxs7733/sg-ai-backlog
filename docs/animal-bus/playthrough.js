const { chromium } = require('playwright');
(async () => {
  const b = await chromium.launch();
  const errs = [];
  for (const [name, vp] of [['land', {width:1024,height:700}], ['port', {width:768,height:950}]]) {
    const p = await b.newPage({ viewport: vp, hasTouch: true });
    p.on('pageerror', e => errs.push(String(e))); p.on('console', m => m.type()==='error' && errs.push(m.text()));
    await p.goto('file://' + require('path').resolve(__dirname, '../../math.html') + '?fast');
    await p.screenshot({ path: `/tmp/animal-bus-${name}-home.png` });
    // jump through all steps
    for (const step of [0, 3, 5, 7]) {
      await p.evaluate(s => { __game.state.step = s; __game.state.hist = []; }, step);
      await p.click('#play'); await p.locator('.dest-btn').first().click({force:true});
      for (let i = 0; i < 5; i++) {
        await p.waitForFunction(() => __game.phase === 'board' && !__game.busy);
        if (i === 0) { await p.waitForTimeout(700); await p.screenshot({ path: `/tmp/animal-bus-${name}-s${step}-board.png` }); }
        // drag first group, tap second
        const g = p.locator('.group').first(); const bb = await g.boundingBox(); const rb = await p.locator('#road').boundingBox();
        await p.mouse.move(bb.x+bb.width/2, bb.y+bb.height/2); await p.mouse.down();
        await p.mouse.move(rb.x+rb.width/2, rb.y+rb.height/2, {steps:5}); await p.mouse.up();
        await p.waitForFunction(() => !__game.busy);
        await p.locator('.group.gb').dispatchEvent('pointerdown',{pointerId:5,clientX:1,clientY:1}); await p.locator('.group.gb').dispatchEvent('pointerup',{pointerId:5,clientX:1,clientY:1});
        await p.waitForFunction(() => __game.phase === 'answer' && !__game.busy);
        const q = await p.evaluate(() => __game.q);
        const seatedN = await p.locator('.seat .tile').count();
        if (seatedN !== q.sum) errs.push(`seated ${seatedN} != ${q.sum}`);
        if (i === 0) { // wrong answer first, then tap count
          const wrong = p.locator(`.choice:not([data-n="${q.sum}"])`).first(); await wrong.click({force:true});
          await p.waitForFunction(() => !__game.busy);
          await p.screenshot({ path: `/tmp/animal-bus-${name}-s${step}-answer.png` });
        }
        await p.click(`.choice[data-n="${q.sum}"]`, {force:true});
        await p.waitForFunction(() => __game.phase !== 'answer');
      }
      await p.waitForFunction(() => __game.phase === 'party');
      console.log(name, 'step', step, '-> now', await p.evaluate(() => __game.state.step), 'stickers', await p.evaluate(() => __game.state.stickers.length));
      if (step === 0) await p.screenshot({ path: `/tmp/animal-bus-${name}-party.png` });
      await p.click('#partyHome');
    }
    await p.close();
  }
  console.log('errors:', errs);
  await b.close();
})();
