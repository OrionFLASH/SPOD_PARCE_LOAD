/* game_fill_ui.js — экраны секций и сбор строк CSV */
"use strict";
function goToContestPeriodBlock(){
  periodNavFocus=true;
  pendingScrollTarget="contest-period-block";
  activeSection="CONTEST";
  closeAllDatePops();
  render();
}
function applyPendingScroll(){
  const id=pendingScrollTarget;
  pendingScrollTarget="";
  if(!id) return;
  requestAnimationFrame(()=>{
    const el=$(id);
    if(el) el.scrollIntoView({behavior:"smooth",block:"start"});
  });
}
function renderContest(){
  ensureJsonStructures(data());
  const ws=$("workspace");
  ws.innerHTML=`${typeCalloutHtml()}<section class="panel">${contestStandEditorHtml(cur())}<h2>Конкурс</h2><p class="intro">Заполняйте сверху вниз: тип → код → сроки → правила. Периоды расчёта — блок <code>CONTEST_PERIOD</code> на этой же странице.</p><button type="button" class="nav-goto" id="btn-goto-feature">Особенности конкурса (FEATURE) →</button><div id="contest-groups"></div></section>`;
  bindContestStandEditor();
  const host=$("contest-groups");
  $("btn-goto-feature")?.addEventListener("click",()=>{periodNavFocus=false;activeSection="CONTEST_FEATURE";closeAllDatePops();render()});
  host.appendChild(renderGrouped(
    "CONTEST", CONTEST_LAYOUT,
    f=>data().contest[f.key]||"",
    (f,v)=>{
      const prevCc=f.key==="CONTEST_CODE"?data().contest.CONTEST_CODE:"";
      data().contest[f.key]=v;
      afterFieldKey(f.key, prevCc);
      persistLocal();
      if(f.key==="CONTEST_TYPE"){render();return}
      if(f.key==="BUSINESS_BLOCK"){refreshContestTabDirty();return}
      if(f.key==="CONTEST_CODE"||f.key==="FULL_NAME"){refreshActiveContestTabTitle();renderNav()}
      if(f.key==="CONTEST_CODE") renderNav();
      refreshContestTabDirty();
    },
    f=>"contest."+f.key,
    null,
    {storeOf:()=>data().contest, leafOf:f=>f.key}
  ));
  const periodHost=document.createElement("div");
  periodHost.id="contest-period-block";
  host.appendChild(periodHost);
  appendJsonArrayEditor(periodHost, "CONTEST_PERIOD", data().contestPeriod, {
    title:"9. Периоды расчёта",
    hint:"JSON-массив CONTEST_PERIOD: несколько наборов period_code / criterion_mark_type / criterion_mark_value. Добавить / дублировать / удалить набор.",
    emptyFactory:emptyContestPeriodItem,
    pathPrefix:"contestPeriod",
    onChange:()=>{persistLocal();refreshContestTabDirty();renderNav()}
  });
  applyPendingScroll();
}
function renderFeature(){
  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml("Особенности конкурса", [contestCtxPill(), {k:"JSON",v:"CONTEST_FEATURE",tip:"Колонка FEATURE у конкурса"}])}
  <p class="intro">JSON <code>CONTEST_FEATURE</code> относится к карточке конкурса (шаг «Карточка» в блоке Конкурс). Среда, отображение, рассылки и видимость.</p>
  <button type="button" class="nav-goto nav-goto--back" id="btn-goto-contest">← К карточке конкурса</button>
  <button type="button" class="nav-goto" id="btn-goto-period-from-feature">Периоды CONTEST_PERIOD →</button>
  <div id="feature-groups"></div>
</section>`;
  $("btn-goto-contest")?.addEventListener("click",()=>{periodNavFocus=false;activeSection="CONTEST";closeAllDatePops();render()});
  $("btn-goto-period-from-feature")?.addEventListener("click",()=>goToContestPeriodBlock());
  $("feature-groups").appendChild(renderGrouped(
    "CONTEST_FEATURE", FEATURE_LAYOUT,
    f=>{
      const leaf=jsonStoreLeaf(f,"CONTEST_FEATURE","FEATURE");
      if(leaf==="businessBlock") return contestBusinessBlockValue();
      return data().feature[leaf]||"";
    },
    (f,v)=>{
      const leaf=jsonStoreLeaf(f,"CONTEST_FEATURE","FEATURE");
      if(leaf==="businessBlock") return;
      data().feature[leaf]=v;persistLocal();refreshContestTabDirty();
    },
    f=>{const leaf=jsonStoreLeaf(f,"CONTEST_FEATURE","FEATURE");return "feature."+leaf},
    null,
    {storeOf:()=>data().feature, leafOf:f=>jsonStoreLeaf(f,"CONTEST_FEATURE","FEATURE")}
  ));
}
function renderContestPeriod(){
  goToContestPeriodBlock();
}
function emptyTableHtml(text, btnId, btnLabel){
  return `<div class="empty-table"><p class="empty-table__text">${esc(text)}</p><button type="button" class="btn btn-primary" id="${esc(btnId)}">${esc(btnLabel)}</button></div>`;
}
function renderEmptyTableSection(title, intro, emptyText, btnId, btnLabel, onAdd){
  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml(title, [contestCtxPill()])}
  <p class="intro">${intro}</p>
  ${emptyTableHtml(emptyText, btnId, btnLabel)}
</section>`;
  $(btnId)?.addEventListener("click", onAdd);
}
function addGroupRow(){
  const rows=data().group;
  rows.push(emptyGroupRow());
  activeGroup=rows.length-1;
  activeSection="GROUP:"+(activeGroup+1);
  persistLocal();
  markContestEdited();
  render();
  toast("Добавлена группа");
}
function addIndicatorRow(){
  const rows=data().indicator;
  rows.push(emptyIndicatorRow());
  activeIndicator=rows.length-1;
  activeSection="INDICATOR:"+(activeIndicator+1);
  persistLocal();
  markContestEdited();
  render();
  toast("Добавлен индикатор");
}
function addScheduleRow(){
  const rows=data().schedule;
  rows.push(emptyScheduleRow());
  activeSchedule=rows.length-1;
  activeSection="SCHEDULE:"+(activeSchedule+1);
  persistLocal();
  markContestEdited();
  render();
  toast("Добавлен турнир");
}
function addPairRow(){
  const max=maxBadges(data().contest.CONTEST_TYPE);
  const curN=data().badges.length;
  if(curN>=max){
    if(!confirm("Для этого типа обычно до "+max+" пар(ы) связь+награда.\nСейчас уже "+curN+". Добавить ещё одну?")) return;
  }
  const preferItem=contestIsItemCatalog(cur())||itemFocusActive();
  const badge=emptyBadge();
  if(preferItem) badge.flat.REWARD_TYPE="ITEM";
  const link=emptyLinkRow();
  const cc=contestCodeOf();
  const rc=preferItem
    ?uniquifyPrefixedCode(buildItemCode(""), liveRewardCodes(data()), "item", cc)
    :uniquifyPrefixedCode(buildPrefixedCode(rewardCodePrefix(cc), ""), liveRewardCodes(data()), "reward", cc);
  data().badges.push(badge);
  data().reward_link.push(link);
  applyRewardCodeToPair(data(), data().badges.length-1, rc);
  syncBusinessBlockFromContest(data());
  activeLink=data().badges.length-1;
  activeBadge=activeLink;
  activeSection="PAIR:"+(activeLink+1);
  activePairFocus=preferItem?activeLink:null;
  persistLocal();markContestEdited();render();
  toast(curN+1>max?("Добавлена пара "+(curN+1)+" (выше рекомендации "+max+")"):"Добавлена пара");
}
function renderLinkRewardPair(){
  syncBadgeSlots(data(),false);
  seedLinked(data());
  const max=maxBadges(data().contest.CONTEST_TYPE);
  const n=data().badges.length;
  if(!n){
    renderEmptyTableSection(
      "Связи + награды",
      "В снимке нет пар связь+награда. Заглушка не создаётся, пока не нажмёте «добавить».",
      "Нет связей и наград. Добавьте пару, если она нужна.",
      "btn-add-pair-empty",
      "добавить связь + награду",
      addPairRow
    );
    return;
  }
  if(activeLink>=n) activeLink=Math.max(0,n-1);
  if(activeLink<0) activeLink=0;
  activeBadge=activeLink;
  const i=activeLink;
  const row=data().reward_link[i];
  const badge=data().badges[i];
  row.CONTEST_CODE=contestCodeOf();
  if(!badge.flat.REWARD_TYPE) badge.flat.REWARD_TYPE="BADGE";
  badge.flat.REWARD_CODE=String(row.REWARD_CODE||"");
  const curRc=String(row.REWARD_CODE||"").trim();
  const typeFace=rewardTypeFace(rewardTypeCode(badge));
  const gCodes=groupCodesOf();

  const tabIdxs=navVisiblePairIndexes();
  const itemOn=itemFocusActive();
  const itemIdxs=contestItemPairIndexes(cur());
  const itemPos=itemOn?itemIdxs.indexOf(i)+1:0;
  const tabs=tabIdxs.map(idx=>{
    const label=pairNavTitle(idx);
    return `<button type="button" class="period-tab${idx===i?" active":""}" data-pair="${idx}"><span class="pt-name" data-tip="${esc(label)}">${esc(label)}</span><span class="pt-x" data-del-pair="${idx}" data-tip="В архив">×</span></button>`;
  }).join("");
  const intro=itemOn
    ?"Товар выбран в списке слева. Конкурс, группы и индикаторы общие; в шапке только эта награда. Другой товар — клик по строке под конкурсом."
    :"Одна вкладка = одна пара: сверху связь, снизу награда того же слота. Код награды: <code>r_</code>+код конкурса или для ITEM — <code>ITEM_</code>+окончание.";
  const meta=itemOn
    ?`Товар ${itemPos} из ${itemIdxs.length} · ${esc(pairNavTitle(i))}`
    :`Пара ${i+1} из ${n} · обычно для типа: до ${max}${n>max?" (сейчас больше рекомендации)":""}`;

  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml("Связи + награды", [
    contestCtxPill(),
    {k:"REWARD",v:curRc,tip:curRc?"Код этой пары":"Задайте окончание REWARD_CODE в блоке связи"},
    {k:"TYPE",v:typeFace,tip:"Тип награды (REWARD_TYPE): "+typeFace}
  ])}
  <p class="intro">${intro}</p>
  <div class="period-bar">
    ${itemOn?"":`<div class="period-tabs">${tabs}</div>`}
    <div class="period-bar__actions">
      <button type="button" class="btn btn-primary" id="btn-add-pair" data-tip="${n>=max?("Рекомендация для типа: до "+max+". Сейчас "+n+" — можно добавить ещё"):("Добавить связь + награду (сейчас "+n+", обычно до "+max+")")}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg>
        добавить связь + награду
      </button>
      <button type="button" class="btn btn-danger-soft" id="btn-del-pair" data-tip="Переместить текущую пару в архив">В архив</button>
      <button type="button" class="btn" id="btn-copy-pairs" ${n<=1?"disabled":""} data-tip="${n<=1?"Нужно больше одной связи — копировать некуда":"Выбрать связи, куда скопировать параметры активной пары. REWARD_CODE у целей не меняется (без дублей)"}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>
        Копировать в остальные
      </button>
    </div>
  </div>
  <p class="period-meta">${meta}</p>
  ${standRowEditorHtml(badgeStands(badge, cur()), {id:"row-stand-editor", label:"Стенд пары"})}
  ${!gCodes.length?`<div class="link-hint">Сначала задайте <code>GROUP_CODE</code> на шаге <strong>«3. Группы»</strong>.</div>`:""}

  <div class="pair-block pair-block--link">
    <div class="pair-block__label"><span>Связь</span><span class="pair-block__hint">GROUP + REWARD_CODE</span></div>
    <div id="link-groups"></div>
  </div>

  <div class="pair-block pair-block--reward">
    <div class="pair-block__label"><span>Награда</span><span class="pair-block__hint">${esc(pairRewardHint(i, badge))}</span></div>
    <div id="reward-groups"></div>
    <button type="button" class="nav-goto" id="btn-goto-add" data-tip="Открыть JSON REWARD_ADD_DATA этой награды">Особенности награды (ADD_DATA) →</button>
  </div>
</section>`;

  bindRowStandEditor("row-stand-editor",()=>{
    const b=(data().badges||[])[activeLink];
    const link=(data().reward_link||[])[activeLink];
    if(link&&b) link.stands=b.stands;
    return b;
  });
  $("workspace").querySelectorAll("[data-pair]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      if(e.target.closest("[data-del-pair]")) return;
      activeLink=Number(btn.getAttribute("data-pair"));
      activeBadge=activeLink;
      activeSection="PAIR:"+(activeLink+1);
      if(isItemBadge((data().badges||[])[activeLink])) activePairFocus=activeLink;
      else activePairFocus=null;
      closeAllDatePops();
      render();
    });
  });
  $("workspace").querySelectorAll("[data-del-pair]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      if(deletePairAt(Number(btn.getAttribute("data-del-pair")))) render();
    });
  });
  $("btn-del-pair")?.addEventListener("click",()=>{
    if(deletePairAt(activeLink)) render();
  });
  const addBtn=$("btn-add-pair");
  if(addBtn) addBtn.addEventListener("click",()=>addPairRow());
  $("btn-copy-pairs")?.addEventListener("click",()=>copyActivePairToOthers());
  $("btn-goto-add")?.addEventListener("click",()=>{activeSection="ADD:"+(i+1);closeAllDatePops();render()});

  const linkLayout=LINK_LAYOUT.map(g=>({...g,items:g.items.map(it=>{
    if(it.pickFromGroups||it.key==="GROUP_CODE") return Object.assign({},it,{pickVariants:gCodes,emptyPickHint:"Сначала задайте GROUP_CODE на шаге «Группы»",pickAllowEmpty:false});
    return it;
  })}));

  $("link-groups").appendChild(renderGrouped(
    "TABLE:REWARD-LINK", linkLayout,
    f=>f.key==="CONTEST_CODE"?contestCodeOf():(row[f.key]||""),
    (f,v)=>{
      if(f.key==="CONTEST_CODE"){row.CONTEST_CODE=contestCodeOf();return}
      if(f.key==="REWARD_CODE"){
        applyRewardCodeToPair(data(), i, v);
        refreshPairTypeChrome(i, badge);
        const tab=$("workspace").querySelector(`.period-tab.active .pt-name`);
        if(tab){const t=pairNavTitle(i);tab.textContent=t;tab.setAttribute("data-tip",t)}
        refreshActiveItemTabTitle();
        renderNav();
        persistLocal();refreshContestTabDirty();
        return;
      }
      row[f.key]=v;
      if(f.key==="GROUP_CODE"){
        const tab=$("workspace").querySelector(`.period-tab.active .pt-name`);
        if(tab){const t=pairNavTitle(i);tab.textContent=t;tab.setAttribute("data-tip",t)}
        renderNav();
      }
      persistLocal();refreshContestTabDirty();
    },
    f=>"reward_link."+i+"."+f.key,
    ["CONTEST_CODE"],
    {storeOf:()=>row, leafOf:f=>f.key}
  ));

  $("reward-groups").appendChild(renderGrouped(
    "REWARD", REWARD_LAYOUT,
    f=>f.key==="REWARD_CODE"?curRc:(badge.flat[f.key]||""),
    (f,v)=>{
      if(f.key==="REWARD_CODE") return;
      if(f.key==="REWARD_TYPE"){
        const prev=String(badge.flat.REWARD_TYPE||"");
        badge.flat.REWARD_TYPE=v;
        const converted=convertRewardCodeForType(String(row.REWARD_CODE||badge.flat.REWARD_CODE||""), prev, v, contestCodeOf());
        const kind=isItemRewardType(v)?"item":"reward";
        const unique=uniquifyPrefixedCode(converted, liveRewardCodesExcept(data(), i), kind, contestCodeOf());
        applyRewardCodeToPair(data(), i, unique);
        refreshPairTypeChrome(i, badge);
        if(isItemBadge(badge)) activePairFocus=i;
        else if(activePairFocus===i) activePairFocus=null;
        persistLocal();refreshContestTabDirty();
        render();
        return;
      }
      badge.flat[f.key]=v;
      if(f.key==="FULL_NAME") refreshActiveItemTabTitle();
      persistLocal();refreshContestTabDirty();
    },
    f=>"badges."+i+".flat."+f.key,
    ["REWARD_CODE"],
    {storeOf:()=>badge.flat, leafOf:f=>f.key}
  ));
}

function renderRewardAdd(){
  syncBadgeSlots(data(),false);
  seedLinked(data());
  const n=data().badges.length;
  if(!n){activeSection="PAIR";renderLinkRewardPair();return}
  if(activeLink>=n) activeLink=Math.max(0,n-1);
  if(activeLink<0) activeLink=0;
  activeBadge=activeLink;
  const i=activeLink;
  const row=data().reward_link[i]||{};
  const badge=data().badges[i];
  if(!badge.add) badge.add={};
  badge.flat.REWARD_CODE=String(row.REWARD_CODE||badge.flat.REWARD_CODE||"");
  const curRc=String(badge.flat.REWARD_CODE||"").trim();
  const label=pairNavTitle(i);

  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml("Особенности награды", [
    contestCtxPill(),
    {k:"PARA",v:itemFocusActive()?("товар "+(contestItemPairIndexes(cur()).indexOf(i)+1)+"/"+contestItemPairIndexes(cur()).length):String(i+1)+"/"+n,tip:itemFocusActive()?"Номер товара ITEM в списке слева":"Номер пары среди связей+наград"},
    {k:"REWARD",v:curRc||label,tip:curRc?"Код этой награды":"Сначала задайте REWARD_CODE в связи пары"}
  ])}
  <p class="intro">JSON <code>REWARD_ADD_DATA</code> только для пары <strong>${esc(label)}</strong>. В шапке у этой пары выделен блок «${i+1}» и пункт ADD — так не перепутаете с другими наградами.</p>
  <button type="button" class="nav-goto nav-goto--back" id="btn-goto-pair">← К связи и карточке награды</button>
  <div id="add-groups"></div>
</section>`;
  $("btn-goto-pair")?.addEventListener("click",()=>{activeSection="PAIR:"+(i+1);closeAllDatePops();render()});
  $("add-groups").appendChild(renderGrouped(
    "REWARD_ADD_DATA", ADD_LAYOUT,
    f=>{
      const leaf=jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD");
      if(leaf==="businessBlock") return contestBusinessBlockValue();
      return badge.add[leaf]||"";
    },
    (f,v)=>{
      const leaf=jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD");
      if(leaf==="businessBlock") return;
      badge.add[leaf]=v;persistLocal();refreshContestTabDirty();
    },
    f=>{const leaf=jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD");return "badges."+i+".add."+leaf},
    null,
    {storeOf:()=>badge.add, leafOf:f=>jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD")}
  ));
}

function renderBadge(){activeSection="PAIR:"+(activeBadge+1);renderLinkRewardPair()}
function renderRewardLink(){activeSection="PAIR:"+(activeLink+1);renderLinkRewardPair()}


function renderGroup(){
  seedLinked(data());
  const rows=data().group;
  if(!rows.length){
    renderEmptyTableSection(
      "Группы",
      "Заполняйте до пар «Связи + награды». Значения <code>GROUP_CODE</code> станут вариантами выбора в связи.",
      "Нет групп. Добавьте группу, если она есть в конкурсе.",
      "btn-add-group-empty",
      "Добавить группу",
      addGroupRow
    );
    return;
  }
  if(activeGroup>=rows.length) activeGroup=rows.length-1;
  if(activeGroup<0) activeGroup=0;
  const i=activeGroup;
  const row=rows[i];
  row.CONTEST_CODE=contestCodeOf();

  const tabs=rows.map((r,idx)=>`<button type="button" class="period-tab${idx===i?" active":""}" data-grp="${idx}"><span class="pt-name" data-tip="${esc(groupTitle(r,idx))}">${esc(groupTitle(r,idx))}</span><span class="pt-x" data-del-grp="${idx}" data-tip="В архив">×</span></button>`).join("");

  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml("Группы", [contestCtxPill()])}
  <p class="intro">Заполняйте до пар «Связи + награды». Значения <code>GROUP_CODE</code> станут вариантами выбора в связи.</p>
  <div class="period-bar">
    <div class="period-tabs">${tabs}</div>
    <div class="period-bar__actions">
      <button type="button" class="btn btn-primary" id="btn-add-group" data-tip="Добавить ещё одну группу">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg>
        Группа
      </button>
      <button type="button" class="btn btn-danger-soft" id="btn-del-group" data-tip="Переместить текущую группу в архив">В архив</button>
      <button type="button" class="btn" id="btn-copy-groups" ${rows.length<=1?"disabled":""} data-tip="${rows.length<=1?"Нужно больше одной группы — копировать некуда":"Выбрать группы, куда скопировать все параметры активной группы"}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>
        Копировать в остальные
      </button>
    </div>
  </div>
  <p class="period-meta">Группа ${i+1} из ${rows.length}</p>
  ${standRowEditorHtml(rowStands(row, cur()), {id:"row-stand-editor", label:"Стенд строки"})}
  <div id="group-groups"></div>
</section>`;

  bindRowStandEditor("row-stand-editor",()=>(data().group||[])[activeGroup]);
  $("workspace").querySelectorAll("[data-grp]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      if(e.target.closest("[data-del-grp]")) return;
      activeGroup=Number(btn.getAttribute("data-grp"));
      activeSection="GROUP:"+(activeGroup+1);
      closeAllDatePops();
      render();
    });
  });
  $("workspace").querySelectorAll("[data-del-grp]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      if(deleteGroupAt(Number(btn.getAttribute("data-del-grp")))) render();
    });
  });
  $("btn-del-group")?.addEventListener("click",()=>{
    if(deleteGroupAt(activeGroup)) render();
  });
  $("btn-add-group").addEventListener("click",()=>addGroupRow());
  $("btn-copy-groups")?.addEventListener("click",()=>copyActiveGroupToOthers());

  $("group-groups").appendChild(renderGrouped(
    "TABLE:GROUP", GROUP_LAYOUT,
    f=>f.key==="CONTEST_CODE"?contestCodeOf():(row[f.key]||""),
    (f,v)=>{
      if(f.key==="CONTEST_CODE"){row.CONTEST_CODE=contestCodeOf();return}
      row[f.key]=v;
      if(f.key==="GROUP_CODE") pruneLinkGroupCodes(data());
      persistLocal();refreshContestTabDirty();
      if(f.key==="GROUP_CODE"||f.key==="GROUP_VALUE"||f.key==="GET_CALC_METHOD"){
        const tab=$("workspace").querySelector(`.period-tab.active .pt-name`);
        if(tab){
          const tip=groupSub(row)?groupTitle(row,i)+" · "+groupSub(row):groupTitle(row,i);
          tab.textContent=groupTitle(row,i);
          tab.setAttribute("data-tip",tip);
        }
      }
      if(
        f.key==="GROUP_CODE"||f.key==="GROUP_VALUE"||f.key==="GET_CALC_METHOD"||
        f.key==="GET_CALC_CRITERION"||f.key==="ADD_CALC_CRITERION"||f.key==="ADD_CALC_CRITERION_2"
      ) renderNav();
    },
    f=>"group."+i+"."+f.key,
    ["CONTEST_CODE"],
    {storeOf:()=>row, leafOf:f=>f.key}
  ));
}


function renderIndicator(){
  seedLinked(data());
  const rows=data().indicator;
  if(!rows.length){
    renderEmptyTableSection(
      "Индикаторы",
      "Несколько индикаторов — вкладки. Код конкурса в заголовке, поля — в карточках.",
      "Нет индикаторов. Добавьте индикатор, если он есть в конкурсе.",
      "btn-add-indicator-empty",
      "Добавить индикатор",
      addIndicatorRow
    );
    return;
  }
  if(activeIndicator>=rows.length) activeIndicator=rows.length-1;
  if(activeIndicator<0) activeIndicator=0;
  const i=activeIndicator;
  const row=rows[i];
  row.CONTEST_CODE=contestCodeOf();

  const tabs=rows.map((r,idx)=>`<button type="button" class="period-tab${idx===i?" active":""}" data-ind="${idx}"><span class="pt-name" data-tip="${esc(indicatorTitle(r,idx))}">${esc(indicatorTitle(r,idx))}</span><span class="pt-x" data-del-ind="${idx}" data-tip="В архив">×</span></button>`).join("");

  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml("Индикаторы", [contestCtxPill()])}
  <p class="intro">Несколько индикаторов — вкладки. Код конкурса в заголовке, поля — в карточках.</p>
  <div class="period-bar">
    <div class="period-tabs">${tabs}</div>
    <div class="period-bar__actions">
      <button type="button" class="btn btn-primary" id="btn-add-indicator" data-tip="Добавить ещё один индикатор">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg>
        Индикатор
      </button>
      <button type="button" class="btn btn-danger-soft" id="btn-del-indicator" data-tip="Переместить текущий индикатор в архив">В архив</button>
    </div>
  </div>
  <p class="period-meta">Индикатор ${i+1} из ${rows.length}</p>
  ${standRowEditorHtml(rowStands(row, cur()), {id:"row-stand-editor", label:"Стенд строки"})}
  <div id="indicator-groups"></div>
</section>`;

  bindRowStandEditor("row-stand-editor",()=>(data().indicator||[])[activeIndicator]);
  $("workspace").querySelectorAll("[data-ind]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      if(e.target.closest("[data-del-ind]")) return;
      activeIndicator=Number(btn.getAttribute("data-ind"));
      activeSection="INDICATOR:"+(activeIndicator+1);
      closeAllDatePops();
      render();
    });
  });
  $("workspace").querySelectorAll("[data-del-ind]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      if(deleteIndicatorAt(Number(btn.getAttribute("data-del-ind")))) render();
    });
  });
  $("btn-del-indicator")?.addEventListener("click",()=>{
    if(deleteIndicatorAt(activeIndicator)) render();
  });
  $("btn-add-indicator").addEventListener("click",()=>addIndicatorRow());

  ensureIndicatorJson(row);
  $("indicator-groups").appendChild(renderGrouped(
    "TABLE:INDICATOR", INDICATOR_LAYOUT,
    f=>f.key==="CONTEST_CODE"?contestCodeOf():(row[f.key]||""),
    (f,v)=>{
      if(f.key==="CONTEST_CODE"){row.CONTEST_CODE=contestCodeOf();return}
      row[f.key]=v;persistLocal();refreshContestTabDirty();
      if(f.key==="INDICATOR_CODE"||f.key==="FULL_NAME"||f.key==="N"){
        const tab=$("workspace").querySelector(`.period-tab.active .pt-name`);
        if(tab){tab.textContent=indicatorTitle(row,i);tab.setAttribute("data-tip",indicatorTitle(row,i))}
      }
    },
    f=>"indicator."+i+"."+f.key,
    ["CONTEST_CODE","INDICATOR_FILTER"],
    {storeOf:()=>row, leafOf:f=>f.key}
  ));
  appendJsonArrayEditor($("indicator-groups"), "INDICATOR_FILTER", row.filter_items, {
    title:"INDICATOR_FILTER",
    hint:"Фильтры атрибутов: code / type / match + condition (список через ;) или value/dt.",
    emptyFactory:emptyIndicatorFilterItem,
    pathPrefix:"indicator."+i+".filter_items",
    onChange:()=>{persistLocal();refreshContestTabDirty();renderNav()}
  });
}

function renderSchedule(){
  ensureScheduleSorted();
  const rows=data().schedule;
  if(!rows.length){
    renderEmptyTableSection(
      "Расписание турнира",
      "Каждый период — карточка. <code>TOURNAMENT_CODE</code> = <code>t_</code> + <code>CONTEST_CODE</code>; при непустом окончании — ещё <code>_</code> + окончание (иначе без хвостового <code>_</code>).",
      "Нет турниров. Добавьте период, если он есть в конкурсе.",
      "btn-add-period-empty",
      "добавить турнир",
      addScheduleRow
    );
    return;
  }
  if(activeSchedule>=rows.length) activeSchedule=rows.length-1;
  if(activeSchedule<0) activeSchedule=0;
  const i=activeSchedule;
  const row=rows[i];
  seedLinked(data());
  row.CONTEST_CODE=contestCodeOf();

  const tabs=rows.map((r,idx)=>{
    const stCls=scheduleStatusNavClass(r.TOURNAMENT_STATUS).replace("nav-btn--","period-tab--");
    const tip=scheduleSub(r)?scheduleTitle(r,idx)+" · "+scheduleSub(r):scheduleTitle(r,idx);
    return `<button type="button" class="period-tab${stCls?" "+stCls:""}${idx===i?" active":""}" data-period="${idx}"><span class="pt-name" data-tip="${esc(tip)}">${esc(scheduleTitle(r,idx))}</span><span class="pt-x" data-del-period="${idx}" data-tip="В архив">×</span></button>`;
  }).join("");

  $("workspace").innerHTML=`
<section class="panel">
  ${panelHeadHtml("Расписание турнира", [contestCtxPill()])}
  <p class="intro">Каждый период — карточка. <code>TOURNAMENT_CODE</code> = <code>t_</code> + <code>CONTEST_CODE</code>; при непустом окончании — ещё <code>_</code> + окончание (иначе без хвостового <code>_</code>).</p>
  <div class="period-bar">
    <div class="period-tabs">${tabs}</div>
    <div class="period-bar__actions">
      <button type="button" class="btn btn-primary" id="btn-add-period" data-tip="Добавить ещё один турнир в расписание">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg>
        добавить турнир
      </button>
      <button type="button" class="btn btn-danger-soft" id="btn-del-period" data-tip="Переместить текущий турнир в архив">В архив</button>
      <button type="button" class="btn" id="btn-copy-schedule" ${rows.length<=1?"disabled":""} data-tip="${rows.length<=1?"Нужно больше одного турнира — копировать некуда":"Выбрать турниры этого конкурса, куда скопировать параметры активного расписания (без TOURNAMENT_CODE)"}">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>
        Копировать в остальные
      </button>
    </div>
  </div>
  <p class="period-meta">Период ${i+1} из ${rows.length}</p>
  ${standRowEditorHtml(rowStands(row, cur()), {id:"row-stand-editor", label:"Стенд строки"})}
  <div id="schedule-groups"></div>
</section>`;

  bindRowStandEditor("row-stand-editor",()=>(data().schedule||[])[activeSchedule]);
  $("workspace").querySelectorAll("[data-period]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      if(e.target.closest("[data-del-period]")) return;
      activeSchedule=Number(btn.getAttribute("data-period"));
      activeSection="SCHEDULE:"+(activeSchedule+1);
      closeAllDatePops();
      render();
    });
  });
  $("workspace").querySelectorAll("[data-del-period]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      if(deleteScheduleAt(Number(btn.getAttribute("data-del-period")))) render();
    });
  });
  $("btn-del-period")?.addEventListener("click",()=>{
    if(deleteScheduleAt(activeSchedule)) render();
  });
  $("btn-add-period").addEventListener("click",()=>addScheduleRow());
  $("btn-copy-schedule")?.addEventListener("click",()=>copyActiveScheduleToOthers());

  ensureScheduleJson(row);
  $("schedule-groups").appendChild(renderGrouped(
    "TABLE:SCHEDULE", SCHEDULE_LAYOUT,
    f=>f.key==="CONTEST_CODE"?contestCodeOf():(row[f.key]||""),
    (f,v)=>{
      if(f.key==="CONTEST_CODE"){row.CONTEST_CODE=contestCodeOf();return}
      if(f.key==="TOURNAMENT_CODE"){
        applyTournamentCodeToRow(row, v);
        persistLocal();refreshContestTabDirty();
        const tab=$("workspace").querySelector(`.period-tab.active .pt-name`);
        if(tab){
          const tip=scheduleSub(row)?scheduleTitle(row,i)+" · "+scheduleSub(row):scheduleTitle(row,i);
          tab.textContent=scheduleTitle(row,i);
          tab.setAttribute("data-tip",tip);
        }
        renderNav();
        return;
      }
      row[f.key]=v;persistLocal();refreshContestTabDirty();
      if(f.key==="TOURNAMENT_STATUS"){
        ensureScheduleSorted();
        persistLocal();
        render();
        return;
      }
      if(f.key==="PERIOD_TYPE"||f.key==="START_DT"){
        const tab=$("workspace").querySelector(`.period-tab.active .pt-name`);
        if(tab){
          const tip=scheduleSub(row)?scheduleTitle(row,i)+" · "+scheduleSub(row):scheduleTitle(row,i);
          tab.textContent=scheduleTitle(row,i);
          tab.setAttribute("data-tip",tip);
        }
        renderNav();
      }
    },
    f=>"schedule."+i+"."+f.key,
    ["CONTEST_CODE","TARGET_TYPE","FILTER_PERIOD_ARR"],
    {storeOf:()=>row, leafOf:f=>f.key}
  ));
  const schExtra=$("schedule-groups");
  appendScheduleTargetTypeEditor(schExtra, row, {onChange:()=>{persistLocal();refreshContestTabDirty();renderNav()}});
  appendJsonArrayEditor(schExtra, "FILTER_PERIOD_ARR", row.filter_period, {
    title:"FILTER_PERIOD_ARR",
    hint:"Элементы фильтра периода: period_code, start_dt, end_dt; опционально criterion_*.",
    emptyFactory:emptyFilterPeriodItem,
    pathPrefix:"schedule."+i+".filter_period",
    onChange:()=>{persistLocal();refreshContestTabDirty();renderNav()}
  });
}


function renderTable(kind,title,cols,rowsKey){
  seedLinked(data());
  const rows=data()[rowsKey];
  const secId=kind==="SCHEDULE"?"TABLE:SCHEDULE":("TABLE:"+kind);
  const cc=contestCodeOf();
  const head=cols.map(c=>{
    const f=meta(secId,c);
    return `<th data-tip="${esc(tipFor(f))}">${esc(c)} ${emptyPillHtml(f)}${jsonRequiredPillHtml(f)}</th>`;
  }).join("")+"<th></th>";
  const body=rows.map((row,ri)=>{
    row.CONTEST_CODE=cc;
    const cells=cols.map(c=>{
      const f=meta(secId,c);
      if(c==="CONTEST_CODE"){
        return `<td><div class="locked-value" data-tip="Код конкурса из шага Конкурс — не меняется"><span>${esc(cc||"—")}</span><span class="locked-value__hint">из Конкурс</span></div></td>`;
      }
      if(f.kind==="dropdown"&&(f.variants||[]).length){
        return `<td><div class="chips" data-r="${ri}" data-c="${esc(c)}">${(f.variants||[]).map(v=>{
          const lab=labelForVariant(f,v);
          return `<button type="button" class="default-chip${String(row[c]||"")===String(v)?" is-on":""}" data-v="${esc(v)}" data-tip="${esc(lab?lab+" → "+v:v)}"><span class="default-chip__mark">${markSvg()}</span>${chipFaceHtml(v,lab)}</button>`;
        }).join("")}</div></td>`;
      }
      if(f.kind==="date") return `<td><input class="cell-input" data-r="${ri}" data-c="${esc(c)}" value="${esc(row[c]||"")}" placeholder="YYYY-MM-DD" data-tip="${esc(tipFor(f))}" /></td>`;
      return `<td><input class="cell-input" data-r="${ri}" data-c="${esc(c)}" value="${esc(row[c]||"")}" data-tip="${esc(tipFor(f))}" /></td>`;
    }).join("");
    return `<tr>${cells}<td><button type="button" class="btn" data-del="${ri}">×</button></td></tr>`;
  }).join("");
  const intro=kind==="REWARD-LINK"
    ? "Задайте связи конкурса с наградами: CONTEST_CODE фиксирован. Укажите REWARD_CODE здесь — на шаге «Награды» выберете их для BADGE."
    : "Строки SPOD. CONTEST_CODE всегда из шага «Конкурс» и не редактируется.";
  $("workspace").innerHTML=`<section class="panel"><h2>${esc(title)}</h2><p class="intro">${intro}</p><div class="table-wrap"><table class="grid"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div><div class="table-actions"><button type="button" class="btn btn-primary" id="btn-add-row"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg>Добавить строку</button></div></section>`;
  $("workspace").querySelectorAll(".cell-input").forEach(el=>{
    const sync=()=>{
      const r=Number(el.getAttribute("data-r"));const c=el.getAttribute("data-c");
      if(c==="CONTEST_CODE")return;
      data()[rowsKey][r][c]=el.value;
      persistLocal();refreshContestTabDirty();
    };
    el.addEventListener("input",sync);el.addEventListener("change",()=>{sync();refreshContestTabDirty()});
  });
  $("workspace").querySelectorAll(".chips").forEach(box=>{
    box.querySelectorAll(".default-chip").forEach(btn=>btn.addEventListener("click",()=>{
      const r=Number(box.getAttribute("data-r"));const c=box.getAttribute("data-c");const v=btn.getAttribute("data-v");
      if(c==="CONTEST_CODE")return;
      const f=meta(secId,c);const cur=data()[rowsKey][r][c];
      if(cur===v){
        if(selectionRequired(f)){toast("«можно не указывать» = нет — требуется указать значение");return}
        data()[rowsKey][r][c]="";
      }else data()[rowsKey][r][c]=v;
      if(rowsKey==="group"&&c==="GROUP_CODE") pruneLinkGroupCodes(data());
      persistLocal();render();
    }));
  });
  $("workspace").querySelectorAll("[data-del]").forEach(btn=>btn.addEventListener("click",()=>{
    const r=Number(btn.getAttribute("data-del"));
    data()[rowsKey].splice(r,1);
    if(rowsKey==="group") pruneLinkGroupCodes(data());
    persistLocal();render();
  }));
  $("btn-add-row").addEventListener("click",()=>{
    const row=Object.fromEntries(cols.map(c=>[c,""]));
    row.CONTEST_CODE=cc;
    data()[rowsKey].push(row);persistLocal();render();
  });
}

function render(){
  if(!catalog||!contests.length)return;
  if(activeArchiveId){
    const entry=findArchiveEntry(activeArchiveId);
    if(!entry) activeArchiveId=null;
  }
  if(!activeArchiveId){
    syncBadgeSlots(data(),false);
    seedLinked(data());
    normalizeActiveSection();
  }
  renderContestTabs();
  if(activeArchiveId){
    const entry=findArchiveEntry(activeArchiveId);
    renderArchiveNav(entry);
    renderArchiveView(entry);
    return;
  }
  if(activeSection==="CONTEST_PERIOD"){
    periodNavFocus=true;
    if(!pendingScrollTarget) pendingScrollTarget="contest-period-block";
    activeSection="CONTEST";
  }
  renderNav();
  if(activeSection==="CONTEST")renderContest();
  else if(activeSection==="CONTEST_FEATURE")renderFeature();
  else if(activeSection.startsWith("ADD:"))renderRewardAdd();
  else if(activeSection.startsWith("PAIR:")||activeSection==="PAIR"||activeSection.startsWith("BADGE:")||activeSection.startsWith("LINK:")||activeSection==="REWARD-LINK")renderLinkRewardPair();
  else if(activeSection==="GROUP"||activeSection.startsWith("GROUP:"))renderGroup();
  else if(activeSection==="INDICATOR"||activeSection.startsWith("INDICATOR:"))renderIndicator();
  else if(activeSection==="SCHEDULE"||activeSection.startsWith("SCHEDULE:"))renderSchedule();
  realignBaselineIfPristine(cur());
}


/* ——— SPOD-JSON массивы: CONTEST_PERIOD / FILTER_PERIOD_ARR / INDICATOR_FILTER / TARGET_TYPE(schedule) ——— */
function normalizeSpodJsonText(raw){
  if(raw==null) return "";
  let s=String(raw).trim();
  if(!s||s==="-"||s==="None"||s==="null") return "";
  // json_array в CSV: весь блок в одинарных кавычках '[{...}]'
  if(s.length>=2&&s[0]==="'"&&s[s.length-1]==="'"){
    const inner=s.slice(1,-1).trim();
    if(inner.startsWith("{")||inner.startsWith("[")) s=inner;
  }
  s=s.replace(/"""/g,'"');
  while(s.length>=2&&s[0]==='"'&&s[s.length-1]==='"'){
    const inner=s.slice(1,-1).trim();
    if(inner.startsWith("{")||inner.startsWith("[")) s=inner;
    else break;
  }
  while(s.length>=2&&s[s.length-1]==='"'&&(s.startsWith("{")||s.startsWith("["))){
    s=s.slice(0,-1).trimEnd();
  }
  return s.trim();
}
function parseSpodJson(raw){
  const norm=normalizeSpodJsonText(raw);
  if(!norm) return null;
  try{return JSON.parse(norm)}catch(_){
    try{
      const m=/^\s*[\[{]/.exec(norm);
      if(!m) return null;
      // raw_decode-подобно: взять первый полный JSON-объект/массив
      let depth=0, inStr=false, esc=false;
      for(let i=0;i<norm.length;i++){
        const ch=norm[i];
        if(inStr){
          if(esc){esc=false;continue}
          if(ch==="\\"){esc=true;continue}
          if(ch==='"') inStr=false;
          continue;
        }
        if(ch==='"'){inStr=true;continue}
        if(ch==="{"||ch==="[") depth++;
        else if(ch==="}"||ch==="]"){
          depth--;
          if(depth===0){
            try{return JSON.parse(norm.slice(0,i+1))}catch(__){return null}
          }
        }
      }
    }catch(__){}
    return null;
  }
}
function asFiniteNumber(v, fallback){
  if(v===null||v===undefined||v==="") return fallback;
  const n=Number(v);
  return Number.isFinite(n)?n:fallback;
}
function emptyContestPeriodItem(){
  const fields=(catalog.sections.find(s=>s.id==="CONTEST_PERIOD")||{fields:[]}).fields;
  const get=(leaf,def)=>{const f=fields.find(x=>(x.key||"").endsWith("."+leaf));return f&&f.default!=null&&String(f.default)!==""?String(f.default):def};
  return{period_code:get("period_code","0"),criterion_mark_type:get("criterion_mark_type",">"),criterion_mark_value:get("criterion_mark_value","0")};
}
function emptyFilterPeriodItem(){
  return{period_code:"1",start_dt:"",end_dt:"",criterion_mark_type:"",criterion_mark_value:""};
}
function emptyIndicatorFilterItem(){
  return{
    filtered_attribute_code:"",
    filtered_attribute_type:"STRING",
    filtered_attribute_match:"IN",
    filtered_attribute_condition:"",
    filtered_attribute_value:"",
    filtered_attribute_dt:""
  };
}
function normalizeContestPeriodItem(raw){
  const it=(raw&&typeof raw==="object"&&!Array.isArray(raw))?raw:{};
  return{
    period_code:String(it.period_code!=null?it.period_code:"0"),
    criterion_mark_type:String(it.criterion_mark_type||">"),
    criterion_mark_value:String(it.criterion_mark_value!=null?it.criterion_mark_value:"0")
  };
}
function normalizeFilterPeriodItem(raw){
  const it=(raw&&typeof raw==="object"&&!Array.isArray(raw))?raw:{};
  return{
    period_code:String(it.period_code!=null?it.period_code:"1"),
    start_dt:String(it.start_dt||""),
    end_dt:String(it.end_dt||""),
    criterion_mark_type:String(it.criterion_mark_type||""),
    criterion_mark_value:it.criterion_mark_value!=null&&it.criterion_mark_value!==""?String(it.criterion_mark_value):""
  };
}
function normalizeIndicatorFilterItem(raw){
  const it=(raw&&typeof raw==="object"&&!Array.isArray(raw))?raw:{};
  let cond=it.filtered_attribute_condition;
  if(Array.isArray(cond)) cond=cond.map(x=>String(x)).join(";");
  else cond=String(cond||"");
  return{
    filtered_attribute_code:String(it.filtered_attribute_code||""),
    filtered_attribute_type:String(it.filtered_attribute_type||"STRING"),
    filtered_attribute_match:String(it.filtered_attribute_match||"IN"),
    filtered_attribute_condition:cond,
    filtered_attribute_value:it.filtered_attribute_value!=null&&it.filtered_attribute_value!==""?String(it.filtered_attribute_value):"",
    filtered_attribute_dt:String(it.filtered_attribute_dt||"")
  };
}
function jsonLeafMeta(sectionId, leaf){
  const fields=(catalog.sections.find(s=>s.id===sectionId)||{fields:[]}).fields;
  return fields.find(f=>{
    const k=String(f.key||"");
    const jt=String(f.json_target||"");
    return k===leaf||k.endsWith("."+leaf)||jt.endsWith("."+leaf)||jt.endsWith("[]."+leaf);
  })||null;
}
/** json_required=false → ключ может отсутствовать: при пустом значении не пишем ключ.
 *  json_required=true → ключ всегда есть; пустое значение допустимо, если allow_empty у поля. */
function putJsonLeaf(out, sectionId, leaf, value, empty){
  const f=jsonLeafMeta(sectionId, leaf);
  if(empty&&!fieldJsonRequired(f)) return; // может отсутствовать + пусто → ключа нет
  out[leaf]=value; // обязателен (или есть значение) → ключ присутствует
}
function packContestPeriodItem(it){
  const o={};
  putJsonLeaf(o,"CONTEST_PERIOD","period_code",asFiniteNumber(it.period_code,0),isEmptyRaw(it.period_code));
  putJsonLeaf(o,"CONTEST_PERIOD","criterion_mark_type",String(it.criterion_mark_type||">"),isEmptyRaw(it.criterion_mark_type));
  putJsonLeaf(o,"CONTEST_PERIOD","criterion_mark_value",asFiniteNumber(it.criterion_mark_value,0),isEmptyRaw(it.criterion_mark_value));
  return o;
}
function packFilterPeriodItem(it){
  const o={};
  putJsonLeaf(o,"FILTER_PERIOD_ARR","period_code",asFiniteNumber(it.period_code,0),isEmptyRaw(it.period_code));
  putJsonLeaf(o,"FILTER_PERIOD_ARR","start_dt",String(it.start_dt||"").trim(),isEmptyRaw(it.start_dt));
  putJsonLeaf(o,"FILTER_PERIOD_ARR","end_dt",String(it.end_dt||"").trim(),isEmptyRaw(it.end_dt));
  putJsonLeaf(o,"FILTER_PERIOD_ARR","criterion_mark_type",String(it.criterion_mark_type||"").trim(),isEmptyRaw(it.criterion_mark_type));
  putJsonLeaf(o,"FILTER_PERIOD_ARR","criterion_mark_value",asFiniteNumber(it.criterion_mark_value,0),isEmptyRaw(it.criterion_mark_value));
  return o;
}
function packIndicatorFilterItem(it){
  const o={};
  putJsonLeaf(o,"INDICATOR_FILTER","filtered_attribute_code",String(it.filtered_attribute_code||""),isEmptyRaw(it.filtered_attribute_code));
  putJsonLeaf(o,"INDICATOR_FILTER","filtered_attribute_type",String(it.filtered_attribute_type||"STRING"),isEmptyRaw(it.filtered_attribute_type));
  putJsonLeaf(o,"INDICATOR_FILTER","filtered_attribute_match",String(it.filtered_attribute_match||"IN"),isEmptyRaw(it.filtered_attribute_match));
  const cond=listFromCell(it.filtered_attribute_condition);
  putJsonLeaf(o,"INDICATOR_FILTER","filtered_attribute_condition",cond,!cond.length);
  const vv=String(it.filtered_attribute_value||"").trim();
  const n=Number(vv);
  putJsonLeaf(o,"INDICATOR_FILTER","filtered_attribute_value",Number.isFinite(n)&&vv!==""?n:vv,isEmptyRaw(it.filtered_attribute_value));
  putJsonLeaf(o,"INDICATOR_FILTER","filtered_attribute_dt",String(it.filtered_attribute_dt||"").trim(),isEmptyRaw(it.filtered_attribute_dt));
  return o;
}
function seasonCodeOfSchedule(row){
  if(!row) return "";
  if(row.seasonCode!=null&&String(row.seasonCode).trim()!=="") return String(row.seasonCode).trim();
  const raw=row.TARGET_TYPE;
  if(raw==null||String(raw).trim()==="") return "";
  const s=String(raw).trim();
  if(s.startsWith("{")||s.includes('"""')||s.includes('"seasonCode"')){
    const p=parseSpodJson(s);
    if(p&&typeof p==="object"&&!Array.isArray(p)) return String(p.seasonCode||"").trim();
  }
  return s;
}
function ensureScheduleJson(row){
  if(!row||typeof row!=="object") return;
  if(row.seasonCode==null||row.seasonCode===""){
    let sc=seasonCodeOfSchedule(row);
    if(!sc){
      const sf=(catalog.sections.find(s=>s.id==="SCHEDULE_TARGET_TYPE")||{fields:[]}).fields[0];
      sc=sf&&sf.default!=null?String(sf.default):"";
    }
    row.seasonCode=sc;
  }
  if("TARGET_TYPE" in row) delete row.TARGET_TYPE;
  if(!Array.isArray(row.filter_period)){
    let src=null;
    if(row.FILTER_PERIOD_ARR!=null&&String(row.FILTER_PERIOD_ARR).trim()){
      src=parseSpodJson(row.FILTER_PERIOD_ARR);
    }
    row.filter_period=Array.isArray(src)?src.map(normalizeFilterPeriodItem):[];
  }
  if("FILTER_PERIOD_ARR" in row) delete row.FILTER_PERIOD_ARR;
}
function ensureIndicatorJson(row){
  if(!row||typeof row!=="object") return;
  if(!Array.isArray(row.filter_items)){
    let src=null;
    if(row.INDICATOR_FILTER!=null&&String(row.INDICATOR_FILTER).trim()){
      src=parseSpodJson(row.INDICATOR_FILTER);
    }
    row.filter_items=Array.isArray(src)?src.map(normalizeIndicatorFilterItem):[];
  }
  if("INDICATOR_FILTER" in row) delete row.INDICATOR_FILTER;
}
function ensureJsonStructures(d){
  if(!d||typeof d!=="object") return;
  if(!d.contest||typeof d.contest!=="object") d.contest={};
  if(!Array.isArray(d.contestPeriod)){
    let src=null;
    if(d.contest.CONTEST_PERIOD!=null&&String(d.contest.CONTEST_PERIOD).trim()){
      const raw=d.contest.CONTEST_PERIOD;
      src=parseSpodJson(raw);
      if(!Array.isArray(src)&&typeof raw==="string"&&raw.includes(";")){
        // старый формат формы через ;
        src=null;
      }
    }
    d.contestPeriod=Array.isArray(src)&&src.length?src.map(normalizeContestPeriodItem):[];
  }
  if("CONTEST_PERIOD" in d.contest) delete d.contest.CONTEST_PERIOD;
  for(const row of d.schedule||[]) ensureScheduleJson(row);
  for(const row of d.indicator||[]) ensureIndicatorJson(row);
}
function arrayFieldLeaf(f, sectionPrefix){
  const k=String(f.key||"");
  if(k.startsWith(sectionPrefix+".")) return k.slice(sectionPrefix.length+1);
  return jsonStoreLeaf(f, sectionPrefix, sectionPrefix);
}
function arrItemSummary(sectionId, item, idx){
  const it=item||{};
  if(sectionId==="CONTEST_PERIOD"){
    return `Период ${it.period_code||"?"} · ${it.criterion_mark_type||""} ${it.criterion_mark_value??""}`.trim();
  }
  if(sectionId==="FILTER_PERIOD_ARR"){
    const range=[it.start_dt,it.end_dt].filter(Boolean).join(" → ")||"даты не заданы";
    return `#${it.period_code||"?"} · ${range}`;
  }
  if(sectionId==="INDICATOR_FILTER"){
    const code=it.filtered_attribute_code||"фильтр";
    const match=it.filtered_attribute_match||"";
    let val=String(it.filtered_attribute_condition||it.filtered_attribute_value||it.filtered_attribute_dt||"").trim();
    if(val.length>42) val=val.slice(0,42)+"…";
    return `${code} ${match}${val?" · "+val:""}`.trim();
  }
  return `Набор ${idx+1}`;
}
function appendJsonArrayEditor(host, sectionId, items, opts){
  opts=opts||{};
  const locked=!!opts.locked;
  const title=opts.title||sectionId;
  const hint=opts.hint||"Несколько наборов в одном JSON-массиве: слева список, справа редактирование выбранного.";
  const emptyFactory=opts.emptyFactory||(()=>({}));
  const onChange=opts.onChange||(()=>{});
  const pathPrefix=opts.pathPrefix||sectionId;
  const sec=catalog.sections.find(s=>s.id===sectionId)||{fields:[]};
  const fields=sec.fields||[];
  const wrap=document.createElement("section");
  wrap.className="group-card arr-block";
  wrap.innerHTML=`<div class="group-card__head"><h3 class="group-card__title">${esc(title)}</h3><p class="group-card__hint">${esc(hint)}</p></div><div class="arr-toolbar"></div><div class="arr-layout"><div class="arr-sets"></div><div class="arr-detail"></div></div>`;
  const toolbar=wrap.querySelector(".arr-toolbar");
  const setsEl=wrap.querySelector(".arr-sets");
  const detail=wrap.querySelector(".arr-detail");
  let activeIdx=items.length?0:-1;

  function paintToolbar(){
    toolbar.innerHTML="";
    const count=document.createElement("span");
    count.className="arr-toolbar__count";
    count.textContent=items.length?`Наборов: ${items.length}`:"Наборов нет";
    toolbar.appendChild(count);
    if(locked) return;
    const add=document.createElement("button");
    add.type="button";add.className="btn btn-primary";
    add.innerHTML=`<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg> Добавить набор`;
    add.addEventListener("click",()=>{
      items.push(emptyFactory());
      activeIdx=items.length-1;
      onChange();
      redraw();
    });
    toolbar.appendChild(add);
  }

  function paintSets(){
    setsEl.innerHTML="";
    if(!items.length){
      const p=document.createElement("p");p.className="arr-empty";
      p.textContent=locked?"Нет элементов.":"Добавьте первый набор JSON-настроек.";
      setsEl.appendChild(p);
      return;
    }
    items.forEach((item,idx)=>{
      const btn=document.createElement("button");
      btn.type="button";
      btn.className="arr-set-btn"+(idx===activeIdx?" is-active":"");
      btn.innerHTML=`<span class="arr-set-btn__idx">Набор ${idx+1}</span><span class="arr-set-btn__sum">${esc(arrItemSummary(sectionId,item,idx))}</span>`;
      btn.addEventListener("click",()=>{activeIdx=idx;redraw()});
      setsEl.appendChild(btn);
    });
  }

  function paintDetail(){
    detail.innerHTML="";
    if(activeIdx<0||activeIdx>=items.length){
      const p=document.createElement("p");p.className="arr-empty";
      p.textContent=locked?"Выберите набор слева.":"Выберите набор слева или добавьте новый.";
      detail.appendChild(p);
      return;
    }
    const item=items[activeIdx];
    const idx=activeIdx;
    const card=document.createElement("div");card.className="arr-item";
    card.innerHTML=`<div class="arr-item__head"><span class="arr-item__title">Набор ${idx+1}: ${esc(arrItemSummary(sectionId,item,idx))}</span><div class="arr-item__actions"></div></div><div class="fields-grid"></div>`;
    const actions=card.querySelector(".arr-item__actions");
    if(!locked){
      const dup=document.createElement("button");dup.type="button";dup.className="btn";dup.textContent="Дублировать";
      dup.setAttribute("data-tip","Скопировать этот набор и открыть копию");
      dup.addEventListener("click",()=>{
        items.splice(idx+1,0,JSON.parse(JSON.stringify(item)));
        activeIdx=idx+1;
        onChange();
        redraw();
      });
      const del=document.createElement("button");del.type="button";del.className="btn btn-danger-soft";del.textContent="Удалить";
      del.setAttribute("data-tip","Удалить набор из массива");
      del.addEventListener("click",()=>{
        items.splice(idx,1);
        activeIdx=items.length?Math.min(idx,items.length-1):-1;
        onChange();
        redraw();
      });
      actions.appendChild(dup);
      actions.appendChild(del);
    }
    const grid=card.querySelector(".fields-grid");
    for(const f of fields){
      const leaf=arrayFieldLeaf(f, sectionId);
      const span=f.kind==="list"||f.kind==="text"?12:(f.kind==="date"?6:4);
      grid.appendChild(renderFieldCard(f, item[leaf]!=null?String(item[leaf]):"", pathPrefix+"."+idx+"."+leaf, v=>{
        item[leaf]=v;onChange();
        // обновить подписи списка наборов без полного сброса фокуса полей
        paintSets();
        const titleEl=card.querySelector(".arr-item__title");
        if(titleEl) titleEl.textContent=`Набор ${idx+1}: ${arrItemSummary(sectionId,item,idx)}`;
      },{span,locked,lockedHint:locked?"Архив — только просмотр":""}));
    }
    detail.appendChild(card);
  }

  function redraw(){
    if(activeIdx>=items.length) activeIdx=items.length?items.length-1:-1;
    if(activeIdx<0&&items.length) activeIdx=0;
    paintToolbar();
    paintSets();
    paintDetail();
  }
  redraw();
  host.appendChild(wrap);
  return wrap;
}
function appendScheduleTargetTypeEditor(host, row, opts){
  opts=opts||{};
  const locked=!!opts.locked;
  const sec=catalog.sections.find(s=>s.id==="SCHEDULE_TARGET_TYPE")||{fields:[]};
  const f=sec.fields[0];
  const wrap=document.createElement("section");
  wrap.className="group-card arr-block";
  wrap.innerHTML=`<div class="group-card__head"><h3 class="group-card__title">TARGET_TYPE (seasonCode)</h3><p class="group-card__hint">JSON объекта расписания: {"""seasonCode""": """…"""}. Не путать с TARGET_TYPE конкурса (ПРОМ/ТЕСТ).</p></div><div class="fields-grid"></div>`;
  const grid=wrap.querySelector(".fields-grid");
  if(f){
    grid.appendChild(renderFieldCard(f, row.seasonCode||"", "schedule.seasonCode", v=>{
      row.seasonCode=v;if(opts.onChange)opts.onChange();
    },{span:12,locked,lockedHint:locked?"Архив — только просмотр":""}));
  }
  host.appendChild(wrap);
}

function dumpsSpod(obj){
  if(obj===null||obj===undefined)return"null";
  if(typeof obj==="boolean")return obj?"true":"false";
  if(typeof obj==="number"){
    if(!Number.isFinite(obj)) return'""""""';
    return Number.isInteger(obj)?String(obj):String(obj);
  }
  if(typeof obj==="string")return'"""'+obj+'"""';
  if(Array.isArray(obj)){
    if(!obj.length)return"[]";
    return"["+obj.map(dumpsSpod).join(", ")+"]";
  }
  if(typeof obj==="object"){
    const keys=Object.keys(obj);
    if(!keys.length)return"{}";
    return"{"+keys.map(k=>dumpsSpod(String(k))+": "+dumpsSpod(obj[k])).join(", ")+"}";
  }
  return'"""'+String(obj)+'"""';
}
/** Скаляр для SPOD: number → число без кавычек; list → всегда массив; иначе строка. */
function coerceSpodPackValue(f, raw, empty){
  if(empty){
    if(f&&f.kind==="number") return 0;
    if(f&&f.kind==="list") return [];
    return "";
  }
  if(f&&f.kind==="list") return listFromCell(raw);
  if(f&&f.kind==="number"){
    const n=Number(raw);
    if(!Number.isFinite(n)) return String(raw??"");
    return Number.isInteger(n)?n:n;
  }
  return String(raw??"");
}
/** Значение для depends_on: плоское поле таблицы или json_key в store. */
function resolveDependValue(dep, ctx){
  if(!dep||typeof dep!=="object") return "";
  const jsonKey=String(dep.json_key||dep.json_path||"").trim();
  const field=String(dep.field||"").trim();
  const table=String(dep.table||dep.section||"").trim().toUpperCase();
  const storeName=String(dep.store||"").trim().toLowerCase();
  if(jsonKey){
    if(storeName==="feature"&&ctx.feature) return ctx.feature[jsonKey];
    if(storeName==="add"&&ctx.add) return ctx.add[jsonKey];
    if(ctx.add&&Object.prototype.hasOwnProperty.call(ctx.add, jsonKey)) return ctx.add[jsonKey];
    if(ctx.feature&&Object.prototype.hasOwnProperty.call(ctx.feature, jsonKey)) return ctx.feature[jsonKey];
  }
  if(!field) return "";
  if(table==="REWARD"||field==="REWARD_TYPE"||field==="REWARD_CODE"){
    const flat=(ctx.badge&&ctx.badge.flat)||ctx.rewardFlat||{};
    return flat[field];
  }
  if(table==="CONTEST"||field.indexOf("CONTEST_")==0){
    return (ctx.contest||{})[field];
  }
  if(table==="TABLE:REWARD-LINK"||table==="REWARD-LINK"){
    return (ctx.link||{})[field];
  }
  if(ctx.add&&Object.prototype.hasOwnProperty.call(ctx.add, field)) return ctx.add[field];
  if(ctx.feature&&Object.prototype.hasOwnProperty.call(ctx.feature, field)) return ctx.feature[field];
  if(ctx.contest&&Object.prototype.hasOwnProperty.call(ctx.contest, field)) return ctx.contest[field];
  return "";
}
/** Все условия depends_on (1–3) должны совпасть (AND). */
function fieldDependsOk(f, ctx){
  const deps=Array.isArray(f&&f.depends_on)?f.depends_on.filter(Boolean):[];
  if(!deps.length) return true;
  return deps.every(dep=>{
    const got=resolveDependValue(dep, ctx);
    const expect=dep.equals!=null?dep.equals:(dep.value!=null?dep.value:"");
    return String(got??"").trim()===String(expect??"").trim();
  });
}
function fieldOmitWhenEmpty(f){return !!(f&&f.omit_when_empty)}
/** Упаковка JSON-листьев FEATURE/ADD с учётом omit / depends / list→array. */
function packJsonLeaves(sectionId, store, prefix, ctx){
  const out={};
  const fields=(catalog.sections.find(s=>s.id===sectionId)||{fields:[]}).fields;
  for(const f of fields){
    if(f.kind==="json") continue;
    if(!fieldDependsOk(f, ctx)) continue;
    const storeLeaf=jsonStoreLeaf(f, sectionId, prefix);
    const packLeaf=jsonPackLeaf(f, sectionId);
    const hasKey=store&&Object.prototype.hasOwnProperty.call(store, storeLeaf);
    const raw=hasKey?store[storeLeaf]:undefined;
    const empty=isEmptyRaw(raw)||(Array.isArray(raw)&&!raw.length);
    if(empty&&fieldOmitWhenEmpty(f)) continue;
    if(empty&&!fieldJsonRequired(f)) continue;
    // Обязательный ключ: пишем даже пустым (без автовыбора значения).
    if(packLeaf==="accuracy"||packLeaf==="minNumber"){
      const n=Number(raw);
      out[packLeaf]=Number.isFinite(n)?n:(empty?0:raw);
    }else out[packLeaf]=coerceSpodPackValue(f, raw, empty);
  }
  return out;
}
function featureObject(d){
  ensureJsonStructures(d);
  return packJsonLeaves("CONTEST_FEATURE", d.feature, "FEATURE", {
    contest:d.contest, feature:d.feature
  });
}
function addObject(add, badge){
  const flat=(badge&&badge.flat)||{};
  return packJsonLeaves("REWARD_ADD_DATA", add||{}, "ADD", {
    add:add||{}, badge:badge||{flat}, rewardFlat:flat, contest:data().contest
  });
}
function contestPeriodPacked(d){
  ensureJsonStructures(d);
  return (d.contestPeriod||[]).map(packContestPeriodItem);
}
/** Режим пустой JSON-колонки из каталога. */
function jsonColumnEmptyMode(tableSectionId, columnKey){
  const f=((catalog.sections.find(s=>s.id===tableSectionId)||{fields:[]}).fields||[]).find(x=>x.key===columnKey&&x.kind==="json");
  const mode=f&&f.empty_json_mode?String(f.empty_json_mode):"empty";
  if(mode==="brackets"||mode==="brackets_quoted") return mode;
  return "empty";
}
function jsonColumnWrapQuotes(tableSectionId, columnKey){
  const f=((catalog.sections.find(s=>s.id===tableSectionId)||{fields:[]}).fields||[]).find(x=>x.key===columnKey&&x.kind==="json");
  if(f&&f.json_wrap_quotes===false) return false;
  if(f&&f.json_wrap_quotes===true) return true;
  // по умолчанию: оборачивать непустой массив в "
  return true;
}
/** Ячейка json_array: по каталогу empty / [] / "…" (не одинарные кавычки). */
function dumpsSpodJsonArrayCell(obj, tableSectionId, columnKey){
  const empty=isSpodJsonEmpty(obj);
  const mode=jsonColumnEmptyMode(tableSectionId, columnKey);
  if(empty){
    if(mode==="brackets"||mode==="brackets_quoted"){
      return mode==="brackets_quoted"?'"[]"':"[]";
    }
    return "";
  }
  const inner=dumpsSpod(obj);
  if(jsonColumnWrapQuotes(tableSectionId, columnKey)){
    // Обёртка SPOD — двойные кавычки (не одинарные). CSV-экранирование — в toCsv.
    return '"'+inner+'"';
  }
  return inner;
}
function contestRow(d){
  ensureJsonStructures(d);
  syncBusinessBlockFromContest(d);
  const row={};for(const c of CONTEST_CSV_COLS)row[c]="";
  for(const[k,v] of Object.entries(d.contest||{})){
    if(k==="CONTEST_PERIOD"||k==="stands") continue;
    if(k==="BUSINESS_BLOCK") row[k]=dumpsSpod(listFromCell(v));
    else if(CONTEST_CSV_COLS.includes(k)) row[k]=String(v??"");
  }
  const feat=featureObject(d);
  row.CONTEST_FEATURE=(isSpodJsonEmpty(feat)&&jsonColumnAllowEmpty("CONTEST","CONTEST_FEATURE"))?"":dumpsSpod(feat);
  const periods=contestPeriodPacked(d);
  row.CONTEST_PERIOD=dumpsSpodJsonArrayCell(periods,"CONTEST","CONTEST_PERIOD");
  return row;
}
function scheduleRows(d){
  ensureJsonStructures(d);
  return (d.schedule||[]).map(r=>{
    const out={};
    SCH_COLS.forEach(c=>{
      if(c==="FILTER_PERIOD_ARR"){
        const packed=(r.filter_period||[]).map(packFilterPeriodItem);
        out[c]=dumpsSpodJsonArrayCell(packed,"TABLE:SCHEDULE","FILTER_PERIOD_ARR");
      }else if(c==="TARGET_TYPE"){
        const sc=String(r.seasonCode||"").trim();
        if(sc) out[c]=dumpsSpod({seasonCode:sc});
        else out[c]=jsonColumnAllowEmpty("TABLE:SCHEDULE","TARGET_TYPE")?"":dumpsSpod({seasonCode:""});
      }else out[c]=String(r[c]??"");
    });
    return out;
  }).filter(r=>SCH_COLS.some(c=>String(r[c]||"").trim()));
}
function indicatorRows(d){
  ensureJsonStructures(d);
  return (d.indicator||[]).map(r=>{
    const out={};
    IND_COLS.forEach(c=>{
      if(c==="INDICATOR_FILTER"){
        const packed=(r.filter_items||[]).map(packIndicatorFilterItem).filter(x=>String(x.filtered_attribute_code||"").trim());
        out[c]=dumpsSpodJsonArrayCell(packed,"TABLE:INDICATOR","INDICATOR_FILTER");
      }else out[c]=String(r[c]??"");
    });
    return out;
  }).filter(r=>IND_COLS.some(c=>String(r[c]||"").trim()));
}
function rewardRows(d){return (d.badges||[]).map(b=>{const row={};for(const c of REWARD_CSV_COLS)row[c]="";for(const[k,v] of Object.entries((b&&b.flat)||{})){if(REWARD_CSV_COLS.includes(k))row[k]=String(v??"")}if(!row.REWARD_TYPE)row.REWARD_TYPE="BADGE";const addObj=addObject((b&&b.add)||{}, b);row.REWARD_ADD_DATA=(isSpodJsonEmpty(addObj)&&jsonColumnAllowEmpty("REWARD","REWARD_ADD_DATA"))?"":dumpsSpod(addObj);return row})}
function tableRows(rows,cols){return rows.filter(r=>cols.some(c=>String(r[c]||"").trim())).map(r=>{const out={};cols.forEach(c=>out[c]=String(r[c]??""));return out})}
function csvEscape(val){
  const s=String(val??"");
  if(s==="") return "";
  // SPOD-JSON: ключи/строки уже в """…""" (как PROM). Не удваивать " → иначе """""" .
  // json_array: обёртка "…" или '…' / голый [] — без повторного CSV-экранирования кавычек.
  const isSpodJson=
    s.includes('"""')||
    s==="[]"||s==="{}"||
    (s.startsWith("'[")&&s.endsWith("]'"))||
    (s.startsWith("'{")&&s.endsWith("}'"))||
    (s.startsWith('"[')&&s.endsWith(']"'))||
    (s.startsWith('"{')&&s.endsWith('}"'))||
    ((s.startsWith("{")||s.startsWith("["))&&(s.includes('"""')||s==="[]"||s==="{}"));
  if(isSpodJson){
    if(/[;\n\r]/.test(s)){
      // разделитель CSV — «;»; в JSON его быть не должно
      return s.replace(/;/g,",");
    }
    return s;
  }
  if(/[;"\n\r]/.test(s)) return'"'+s.replace(/"/g,'""')+'"';
  return s;
}
function toCsv(rows,cols){const lines=[cols.join(";")];for(const row of rows)lines.push(cols.map(c=>csvEscape(row[c])).join(";"));return lines.join("\r\n")+"\r\n"}
function csvBlob(text){
  const bom=new Uint8Array([0xef,0xbb,0xbf]);
  const body=new TextEncoder().encode(text);
  const bytes=new Uint8Array(bom.length+body.length);
  bytes.set(bom,0);bytes.set(body,bom.length);
  return new Blob([bytes],{type:"text/csv;charset=utf-8"});
}
/** Скачивание через <a download>. revoke откладываем — иначе при «Все 6» браузер часто рвёт 2–3-й файл (часто REWARD). */
function downloadCsv(filename,text){
  return new Promise(resolve=>{
    const blob=csvBlob(text);
    const url=URL.createObjectURL(blob);
    const a=document.createElement("a");
    a.href=url;
    a.download=filename;
    a.rel="noopener";
    a.style.display="none";
    document.body.appendChild(a);
    a.click();
    setTimeout(()=>{
      try{a.remove()}catch(_){}
      try{URL.revokeObjectURL(url)}catch(_){}
      resolve();
    },1800);
  });
}
async function writeCsvToDirectory(dirHandle,filename,text){
  const handle=await dirHandle.getFileHandle(filename,{create:true});
  const writable=await handle.createWritable();
  try{
    await writable.write(csvBlob(text));
  }finally{
    await writable.close();
  }
}
function exportStamp(){const d=new Date();const p=n=>String(n).padStart(2,"0");return `${d.getFullYear()}${p(d.getMonth()+1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}`}
function fileName(key,ts){const stamp=ts||exportStamp();const map={contest:`CONTEST (${BLOCK}) FORM_FILL_${stamp}.csv`,reward:`REWARD (${BLOCK}) FORM_FILL_${stamp}.csv`,reward_link:`REWARD-LINK (${BLOCK}) FORM_FILL_${stamp}.csv`,group:`GROUP (${BLOCK}) FORM_FILL_${stamp}.csv`,indicator:`INDICATOR (${BLOCK}) FORM_FILL_${stamp}.csv`,schedule:`SCHEDULE (${BLOCK}) FORM_FILL_${stamp}.csv`};return map[key]}
var EXPORT_KEYS=["contest","reward","reward_link","group","indicator","schedule"];
function exportCols(){
  return {contest:CONTEST_CSV_COLS,reward:REWARD_CSV_COLS,reward_link:LINK_COLS,group:GROUP_COLS,indicator:IND_COLS,schedule:SCH_COLS};
}
/** CSV только из активных contests — archiveEntries не включаются. */
