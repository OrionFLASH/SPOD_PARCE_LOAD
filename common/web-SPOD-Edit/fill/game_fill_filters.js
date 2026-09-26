/* game_fill_filters.js — поля ввода, фильтры списка, выбор конкурсов, навигация */
"use strict";
function mountFullCodeField(host, f, fullValue, onChange, tip, kind){
  const wrap=document.createElement("div");
  wrap.className="code-full-wrap";
  const fmt=kind==="item"?"ITEM_ + окончание товара":((kind==="reward"?"r_":"t_")+" + CONTEST_CODE");
  wrap.setAttribute("data-tip", tip+"\nКод не в формате "+fmt+" — правится целиком, без префикса и окончания.");
  const inp=document.createElement("input");
  inp.type="text";
  inp.className="code-full-input";
  inp.value=String(fullValue||"");
  inp.placeholder="полный код";
  inp.setAttribute("data-tip", wrap.getAttribute("data-tip"));
  const note=document.createElement("div");
  note.className="code-composite__full";
  note.textContent=kind==="item"
    ?"префикс ITEM_ не найден — редактируется весь код"
    :"формат r_/t_ + код конкурса не найден — редактируется весь код";
  inp.addEventListener("input",()=>onChange(inp.value));
  inp.addEventListener("focus",()=>host.closest(".field")?.classList.add("is-focus"));
  inp.addEventListener("blur",()=>{
    host.closest(".field")?.classList.remove("is-focus");
    onChange(String(inp.value||"").trim());
  });
  wrap.appendChild(inp);
  wrap.appendChild(note);
  host.appendChild(wrap);
}

function mountCompositeCode(host, f, fullValue, onChange, tip, prefix, opts){
  opts=opts||{};
  const sticky=!!opts.sticky;
  const cc=contestCodeOf();
  const miss=!sticky&&!cc;
  const wrap=document.createElement("div");
  wrap.className="code-composite-wrap";
  const box=document.createElement("div");
  box.className="code-composite";
  const glueTip=sticky
    ?"Только окончание товара; префикс ITEM_ фиксирован"
    :"Только окончание; пусто → код без «_» в конце (r_/t_ + CONTEST_CODE)";
  box.setAttribute("data-tip",tip+(miss?"\nСначала задайте CONTEST_CODE на шаге «Конкурс»":"\n"+glueTip));
  const pre=document.createElement("span");
  pre.className="code-composite__prefix"+(miss?" is-miss":"");
  pre.setAttribute("data-tip",sticky?"База: ITEM_ + окончание товара":(miss?"Нужен код конкурса":"База: r_/t_ + CONTEST_CODE. Символ «_» перед окончанием — только если окончание заполнено"));
  const inp=document.createElement("input");
  inp.type="text";
  inp.placeholder=sticky?"окончание товара":"пусто = без окончания";
  inp.disabled=miss;
  let suf="";
  const full=String(fullValue||"");
  if(sticky){
    suf=followsItemPrefix(full)?endingFromItemCode(full):normalizeCodeSuffix(full);
  }else{
    const cut=suffixAfterPrefix(full, prefix);
    if(cut!==null) suf=cut;
    else if(full.startsWith("r_")||full.startsWith("t_")){
      const rest=full.slice(2);
      if(cc && rest.startsWith(cc)) suf=normalizeCodeSuffix(rest.slice(cc.length));
    }else if(cc && full.startsWith(cc)) suf=normalizeCodeSuffix(full.slice(cc.length));
    else if(full && !full.startsWith("r_") && !full.startsWith("t_")) suf=normalizeCodeSuffix(full);
  }
  inp.value=suf;
  const fullEl=document.createElement("div");
  fullEl.className="code-composite__full";
  function builtCode(){
    if(sticky) return buildItemCode(inp.value);
    if(miss) return "";
    return buildPrefixedCode(prefix, inp.value);
  }
  function paintPrefix(){
    if(sticky){pre.textContent=ITEM_CODE_PREFIX;return}
    if(miss){pre.textContent=String(prefix||"").slice(0,2)+"…";return}
    const s=normalizeCodeSuffix(inp.value);
    pre.textContent=s?(prefix+"_"):prefix;
  }
  function paintFull(){
    paintPrefix();
    const built=builtCode();
    fullEl.textContent=built?("полный код: "+built):(miss?"сначала CONTEST_CODE":"полный код: "+prefix);
  }
  const emit=()=>{onChange(builtCode());paintFull()};
  inp.addEventListener("input",emit);
  inp.addEventListener("focus",()=>host.closest(".field")?.classList.add("is-focus"));
  inp.addEventListener("blur",()=>{host.closest(".field")?.classList.remove("is-focus");emit()});
  box.appendChild(pre);box.appendChild(inp);
  wrap.appendChild(box);wrap.appendChild(fullEl);
  host.appendChild(wrap);
  paintFull();
  if(!miss){
    const built=builtCode();
    if(built && built!==full) withSilentNormalize(()=>onChange(built));
  }
}

function renderFieldCard(f, value, path, onChange, opts){
  opts=opts||{};
  const kind=f.kind||"text";
  const tip=tipFor(f,opts);
  const dirty=isEditedValue(path,value);
  const jsonFact=String(f.json_target||"").trim();
  const card=document.createElement("div");
  const span=opts.span||12;
  const locked=!!opts.locked;
  const pick=Array.isArray(opts.pickVariants)?opts.pickVariants:null;
  const allow=fieldAllowEmpty(f,opts);
  const compositeKind=opts.compositeKind||"";
  const storeObj=opts.storeObj||null;
  const storeLeaf=opts.storeLeaf!=null?opts.storeLeaf:f.key;
  const reqIssue=fieldRequirementIssue(f, storeObj, storeLeaf, opts);
  card.className="field span-"+span+(opts.hero?" field--hero":"")+(dirty?" is-edited":"")+(locked?" is-locked":"")+(pick?" field--pick":"")+(allow?"":" is-required")+(reqIssue?" is-req-warn":"");
  const showKey=jsonFact||String(f.key||"");const showJson=jsonFact&&jsonFact!==String(f.key||"");card.innerHTML=`<div class="field-head"><span class="field-label">${requirementBadgeHtml(reqIssue)}${esc(f.label||f.key)}</span><span class="edited-badge">изменено</span>${emptyPillHtml(f,opts)}${jsonRequiredPillHtml(f)}<span class="field-key" data-tip="Ключ параметра / путь в JSON">${esc(showKey)}</span>${showJson?`<span class="json-fact" data-tip="Полный путь в JSON SPOD: ${esc(jsonFact)}">${esc(jsonFact)}</span>`:""}<span class="kind-pill ${esc(kind)}" data-tip="${esc(kindLabel(kind))}">${esc(kindShort(kind))}</span></div>${reqIssue?`<div class="field-req-msg">${esc(reqIssue.text)}</div>`:""}${f.description?`<div class="field-desc">${esc(f.description)}</div>`:""}<div class="field-control"></div>`;
  const host=card.querySelector(".field-control");
  const wrapChange=v=>{
    onChange(v);
    if(suppressDirtyMark>0) realignBaselineIfPristine(cur());
    else refreshContestTabDirty();
    card.classList.toggle("is-edited",isEditedValue(path,v));
  };
  if(locked&&!pick){
    const raw=lockedValueText(value);
    const lab=labelForVariant(f, raw);
    const show=lab?(lab+(raw?" · "+raw:"")):(raw||"—");
    const box=document.createElement("div");box.className="locked-value";
    box.innerHTML=`<span>${esc(show)}</span>${opts.lockedHint?`<span class="locked-value__hint">${esc(opts.lockedHint)}</span>`:""}`;
    box.setAttribute("data-tip",(opts.lockedHint||"Только просмотр")+"\n"+tip+(raw&&lab?("\nВ файл: "+raw):""));
    host.appendChild(box);
    return card;
  }
  if(pick){
    if(!pick.length){
      const box=document.createElement("div");box.className="locked-value";
      box.innerHTML=`<span>${esc(value||"—")}</span><span class="locked-value__hint">${esc(opts.emptyPickHint||opts.lockedHint||"Нет вариантов для выбора")}</span>`;
      host.appendChild(box);
    }else{
      const fPick=Object.assign({},f,{kind:"dropdown",variants:pick,allow_empty:!!opts.pickAllowEmpty});
      mountChips(host,fPick,value,wrapChange,{multi:false});
    }
    return card;
  }
  if(compositeKind==="reward"||compositeKind==="tournament"){
    const cc=contestCodeOf();
    const raw=String(value||"").trim();
    if(compositeKind==="reward"){
      const rt=pairRewardTypeOf(data(), activeLink);
      if(isItemRewardType(rt)){
        if(raw && !followsItemPrefix(raw)) mountFullCodeField(host,f,value,wrapChange,tip,"item");
        else mountCompositeCode(host,f,value,wrapChange,tip,ITEM_CODE_PREFIX,{sticky:true});
        return card;
      }
    }
    const prefix=compositeKind==="reward"?rewardCodePrefix(cc):tournamentCodePrefix(cc);
    if(raw && !followsPrefixedPrinciple(raw, compositeKind, cc)){
      mountFullCodeField(host,f,value,wrapChange,tip,compositeKind);
    }else{
      mountCompositeCode(host,f,value,wrapChange,tip,prefix);
    }
    return card;
  }
  if(kind==="dropdown_custom") mountDropdownCustom(host,f,value,wrapChange);
  else if(kind==="dropdown"&&(f.variants||[]).length>=COMBOBOX_MIN_VARIANTS) mountSearchCombobox(host,f,value,wrapChange);
  else if(kind==="dropdown"&&(f.variants||[]).length) mountChips(host,f,value,wrapChange,{multi:false});
  else if(kind==="list"){
    if((f.variants||[]).length) mountChips(host,f,value,(v)=>wrapChange(v),{multi:true});
    else mountListFreeform(host,f,value,wrapChange);
  }
  else if(kind==="date") mountDateUi(host,value,wrapChange,tip);
  else if(kind==="number"){
    const inp=document.createElement("input");
    inp.type="number";
    inp.inputMode="decimal";
    inp.step=(f.key==="REWARD_COST"||f.key==="PLAN_MOD_VALUE")?"1":"any";
    if(f.key==="REWARD_COST"){inp.min="0"}
    inp.value=value!==undefined&&value!==null&&String(value)!==""?String(value):"";
    inp.placeholder="число";
    inp.setAttribute("data-tip",tip);
    inp.addEventListener("input",()=>wrapChange(inp.value));
    inp.addEventListener("focus",()=>card.classList.add("is-focus"));
    inp.addEventListener("blur",()=>{
      card.classList.remove("is-focus");
      const raw=String(inp.value||"").trim();
      if(raw===""){if(!fieldAllowEmpty(f,opts)){inp.value=f.default!=null?String(f.default):"0";wrapChange(inp.value)}else wrapChange("");return}
      const n=Number(raw);
      if(!Number.isFinite(n)){toast("Введите число");inp.value=value||"";return}
      const next=(f.key==="REWARD_COST"||f.key==="PLAN_MOD_VALUE")?String(Math.trunc(n)):String(n);
      inp.value=next;wrapChange(next);
    });
    host.appendChild(inp);
  }
  else if(kind==="json"){const ta=document.createElement("textarea");ta.value=value||"";ta.rows=4;ta.placeholder="{ … } или [ … ]";ta.setAttribute("data-tip",tip);ta.addEventListener("input",()=>wrapChange(ta.value));ta.addEventListener("focus",()=>card.classList.add("is-focus"));ta.addEventListener("blur",()=>card.classList.remove("is-focus"));host.appendChild(ta)}
  else if(f.key==="CONTEST_DESCRIPTION"||f.key==="REWARD_DESCRIPTION"||f.key==="REWARD_CONDITION"||String(f.key).endsWith("News")||f.key==="ADD.rewardRule"||f.key==="REWARD_ADD_DATA.rewardRule"||f.key==="ADD.winCriterion"||f.key==="REWARD_ADD_DATA.winCriterion"){
    const ta=document.createElement("textarea");ta.value=value||"";ta.rows=4;ta.setAttribute("data-tip",tip);ta.addEventListener("input",()=>wrapChange(ta.value));ta.addEventListener("focus",()=>card.classList.add("is-focus"));ta.addEventListener("blur",()=>card.classList.remove("is-focus"));host.appendChild(ta);
  }
  else{const inp=document.createElement("input");inp.type="text";inp.value=value||"";inp.setAttribute("data-tip",tip);inp.addEventListener("input",()=>wrapChange(inp.value));inp.addEventListener("focus",()=>card.classList.add("is-focus"));inp.addEventListener("blur",()=>card.classList.remove("is-focus"));host.appendChild(inp)}
  return card;
}

function afterFieldKey(key, prevContestCode){
  const d=data();
  if(key==="CONTEST_TYPE"){syncBadgeSlots(d,true);seedLinked(d)}
  if(key==="CONTEST_CODE"){
    resyncPrefixedCodes(d, prevContestCode==null?"":prevContestCode);
    seedLinked(d);
  }
  if(key==="BUSINESS_BLOCK") syncBusinessBlockFromContest(d);
}

function contestTitle(c,i){
  const code=c.data.contest.CONTEST_CODE||"";
  const name=c.data.contest.FULL_NAME||"";
  if(code||name)return (code||"без кода")+(name?" · "+name:"");
  return "Конкурс "+(i+1);
}
function contestCodeLine(c,i){
  const code=String((c.data.contest&&c.data.contest.CONTEST_CODE)||"").trim();
  if(code) return code;
  return "Конкурс "+((i||0)+1);
}
/** Число наград (пар BADGE), турниров и групп для строки списка. */
function contestRewardCount(c){
  return (c&&c.data&&Array.isArray(c.data.badges))?c.data.badges.length:0;
}
function contestScheduleCount(c){
  return (c&&c.data&&Array.isArray(c.data.schedule))?c.data.schedule.length:0;
}
function contestGroupCount(c){
  return (c&&c.data&&Array.isArray(c.data.group))?c.data.group.length:0;
}
/** HTML 1-й строки вкладки: код · R: n · T: m · G: k */
function contestCodeLineHtml(c,i){
  const code=esc(contestCodeLine(c,i));
  const r=contestRewardCount(c);
  const t=contestScheduleCount(c);
  const g=contestGroupCount(c);
  return code+
    `<span class="ct-dash" aria-hidden="true">-</span><span class="ct-stat">R:&nbsp;${r}</span>`+
    `<span class="ct-sep" aria-hidden="true">·</span><span class="ct-stat">T:&nbsp;${t}</span>`+
    `<span class="ct-sep" aria-hidden="true">·</span><span class="ct-stat">G:&nbsp;${g}</span>`;
}
function contestNameLine(c){
  return String((c.data.contest&&c.data.contest.FULL_NAME)||"").trim();
}
/** Подсказка вкладки: FULL_NAME без кода; если названия нет — код. */
function contestMenuTip(c,i){
  const name=contestNameLine(c);
  const code=contestCodeLine(c,i);
  const stats="R: "+contestRewardCount(c)+" · T: "+contestScheduleCount(c)+" · G: "+contestGroupCount(c);
  if(name) return name+" · "+stats;
  return code+" · "+stats;
}
/** Награды = индивидуальные; Турниры = ТУРНИРНЫЙ (и прочие). */
function contestMenuKind(c){
  const t=String((c.data.contest&&c.data.contest.CONTEST_TYPE)||"").trim().toUpperCase();
  if(t==="ИНДИВИДУАЛЬНЫЙ"||t==="ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ") return "reward";
  return "tournament";
}

/** Обновить заголовок вкладки конкурса без полной перерисовки workspace (иначе текстовые поля теряют фокус). */
function refreshActiveContestTabTitle(){
  const host=$("contest-tabs");
  if(!host)return;
  const tab=host.querySelector(`.contest-tab[data-ci="${activeContest}"]:not([data-pair]) .ct-text`);
  if(tab){
    const c=cur();
    const codeEl=tab.querySelector(".ct-code");
    const nameEl=tab.querySelector(".ct-fullname");
    if(codeEl) codeEl.innerHTML=contestCodeLineHtml(c,activeContest);
    if(nameEl) nameEl.textContent=contestNameLine(c);
    tab.setAttribute("data-tip",contestMenuTip(c,activeContest));
  }
  refreshActiveItemTabTitle();
}
function refreshActiveItemTabTitle(){
  if(activePairFocus==null) return;
  const tab=$("contest-tabs")?.querySelector(`.contest-tab[data-ci="${activeContest}"][data-pair="${activePairFocus}"]`);
  if(!tab) return;
  const c=cur();
  const codeEl=tab.querySelector(".ct-code");
  const nameEl=tab.querySelector(".ct-fullname");
  if(codeEl) codeEl.innerHTML=itemCodeLineHtml(c,activePairFocus);
  if(nameEl) nameEl.textContent=itemListName(c,activePairFocus);
  const amt=itemListAmount(c,activePairFocus);
  const tip=(itemListName(c,activePairFocus)||itemListCode(c,activePairFocus))+(amt?" · Ct: "+amt:"");
  tab.setAttribute("data-tip",tip);
  const text=tab.querySelector(".ct-text");
  if(text) text.setAttribute("data-tip",tip);
}

function searchFieldTokens(parts){
  return (parts||[]).map(x=>String(x??"").trim().toLowerCase()).filter(Boolean);
}
function tokensMatchQuery(tokens, q, mode){
  const needle=String(q||"").trim().toLowerCase();
  if(!needle) return true;
  const m=mode||contestListSearchMode||"contains";
  return (tokens||[]).some(t=>{
    if(m==="starts") return t.startsWith(needle);
    if(m==="equals") return t===needle;
    return t.includes(needle);
  });
}
function contestSearchTokens(c){
  const d=c&&c.data?c.data:{};
  const parts=[];
  const ct=d.contest||{};
  parts.push(ct.CONTEST_CODE,ct.FULL_NAME,ct.CONTEST_DESCRIPTION,ct.BUSINESS_BLOCK);
  parts.push(d.feature&&d.feature.businessBlock);
  for(const b of d.badges||[]){
    const flat=b&&b.flat?b.flat:{};
    const add=b&&b.add?b.add:{};
    parts.push(flat.FULL_NAME,flat.REWARD_DESCRIPTION,flat.REWARD_CODE,flat.REWARD_TYPE,add.businessBlock,add.itemAmount);
  }
  for(const link of d.reward_link||[]){
    parts.push(link&&link.REWARD_CODE);
  }
  for(const row of d.schedule||[]){
    parts.push(row&&row.TOURNAMENT_CODE);
  }
  return searchFieldTokens(parts);
}
function contestMatchesListQuery(c,q){
  return tokensMatchQuery(contestSearchTokens(c), q);
}
function contestMatchesKindFilter(c){
  const kind=contestMenuKind(c);
  if(kind==="reward") return !!contestListShowReward;
  return !!contestListShowTournament;
}
function contestEnvHaystack(c){
  const d=c&&c.data?c.data:{};
  return [String((d.feature&&d.feature.vid)||""), String((d.contest&&d.contest.TARGET_TYPE)||"")].join(" ").toLowerCase();
}
function contestMatchesEnvFilter(c){
  if(contestListShowProm===contestListShowTest) return true;
  const hay=contestEnvHaystack(c);
  if(contestListShowProm) return hay.includes("пром");
  return hay.includes("тест");
}
function splitBusinessBlockTokens(raw){
  const out=[];
  function push(v){
    if(v==null) return;
    if(Array.isArray(v)){v.forEach(push);return}
    if(typeof v==="object"){Object.values(v).forEach(push);return}
    const s=String(v).trim();
    if(!s) return;
    if((s.startsWith("[")||s.startsWith("{"))&&(s.endsWith("]")||s.endsWith("}"))){
      try{push(JSON.parse(s.replace(/'/g,'"')));return}catch(_){}
    }
    if(s.includes(";")){
      s.split(";").forEach(p=>{const t=p.trim();if(t) out.push(t.toUpperCase())});
      return;
    }
    out.push(s.toUpperCase());
  }
  push(raw);
  return out;
}
function businessBlockNamedSet(){
  return new Set(BUSINESS_BLOCK_NAMED.map(x=>String(x).toUpperCase()));
}
function tokensToBbBuckets(toks){
  const named=businessBlockNamedSet();
  const buckets=new Set();
  if(!toks.length){buckets.add(FILTER_EMPTY);return buckets}
  for(const t of toks){
    if(!t){buckets.add(FILTER_EMPTY);continue}
    if(named.has(t)) buckets.add(t);
    else buckets.add(FILTER_BB_OTHER);
  }
  return buckets;
}
function contestBusinessBlockTokens(c){
  const d=c&&c.data?c.data:{};
  const out=[];
  out.push(...splitBusinessBlockTokens(d.contest&&d.contest.BUSINESS_BLOCK));
  out.push(...splitBusinessBlockTokens(d.feature&&d.feature.businessBlock));
  for(const b of d.badges||[]){
    out.push(...splitBusinessBlockTokens(b&&b.add&&b.add.businessBlock));
  }
  return out;
}
function contestMatchesBusinessBlockFilter(c){
  if(!contestListBusinessBlocks.size) return false;
  const buckets=tokensToBbBuckets(contestBusinessBlockTokens(c));
  for(const b of buckets){
    if(contestListBusinessBlocks.has(b)) return true;
  }
  return false;
}
function itemMatchesBusinessBlockFilter(c, pairIdx){
  if(!contestListBusinessBlocks.size) return false;
  const b=((c&&c.data&&c.data.badges)||[])[pairIdx];
  let toks=splitBusinessBlockTokens(b&&b.add&&b.add.businessBlock);
  if(!toks.length){
    const d=(c&&c.data)||{};
    toks=splitBusinessBlockTokens(d.contest&&d.contest.BUSINESS_BLOCK)
      .concat(splitBusinessBlockTokens(d.feature&&d.feature.businessBlock));
  }
  const buckets=tokensToBbBuckets(toks);
  for(const x of buckets){
    if(contestListBusinessBlocks.has(x)) return true;
  }
  return false;
}
function contestScheduleRows(c){
  const sch=c&&c.data?c.data.schedule:undefined;
  return Array.isArray(sch)?sch:[];
}
/** Нет листа/массива, пустой массив, либо строки без статуса и кода турнира. */
function contestScheduleIsEmpty(c){
  const rows=contestScheduleRows(c);
  if(!rows.length) return true;
  return rows.every(row=>{
    const st=String((row&&row.TOURNAMENT_STATUS)||"").trim();
    const tc=String((row&&row.TOURNAMENT_CODE)||"").trim();
    return !st&&!tc;
  });
}
function contestMatchesStatusFilter(c){
  if(!contestListStatuses.size) return false;
  if(contestScheduleIsEmpty(c)) return contestListStatuses.has(FILTER_EMPTY);
  return contestScheduleRows(c).some(row=>{
    const st=String((row&&row.TOURNAMENT_STATUS)||"").trim();
    if(!st) return contestListStatuses.has(FILTER_EMPTY);
    return contestListStatuses.has(st);
  });
}
function contestRewardTypeCodes(c){
  const out=[];
  const badges=(c&&c.data&&c.data.badges);
  if(!Array.isArray(badges)||!badges.length) return out;
  for(const b of badges){
    const t=String((b&&b.flat&&b.flat.REWARD_TYPE)||"").trim();
    if(t) out.push(t);
  }
  return out;
}
function isItemBadge(badge){
  return String((badge&&badge.flat&&badge.flat.REWARD_TYPE)||"").trim()==="ITEM";
}
function contestItemPairIndexes(c){
  const badges=(c&&c.data&&c.data.badges)||[];
  const out=[];
  badges.forEach((b,i)=>{if(isItemBadge(b)) out.push(i)});
  return out;
}
function contestIsItemCatalog(c){
  const badges=(c&&c.data&&c.data.badges)||[];
  return !!badges.length&&badges.every(isItemBadge);
}
function itemListCode(c, pairIdx){
  const b=((c&&c.data&&c.data.badges)||[])[pairIdx];
  const code=String((b&&b.flat&&b.flat.REWARD_CODE)||"").trim();
  if(code) return code;
  const link=(((c&&c.data&&c.data.reward_link)||[])[pairIdx])||{};
  const rc=String(link.REWARD_CODE||"").trim();
  return rc||("Товар "+(pairIdx+1));
}
function itemListName(c, pairIdx){
  const b=((c&&c.data&&c.data.badges)||[])[pairIdx];
  return String((b&&b.flat&&b.flat.FULL_NAME)||"").trim();
}
function itemListAmount(c, pairIdx){
  const b=((c&&c.data&&c.data.badges)||[])[pairIdx];
  const add=(b&&b.add)||{};
  const v=add.itemAmount!=null?add.itemAmount:add.item_amount;
  const s=String(v??"").trim();
  return s;
}
function itemCodeLineHtml(c, pairIdx){
  const code=esc(itemListCode(c, pairIdx));
  const amt=itemListAmount(c, pairIdx);
  const shown=amt||"—";
  return code+`<span class="ct-dash" aria-hidden="true">-</span><span class="ct-stat">Ct:&nbsp;${esc(shown)}</span>`;
}
function itemListTokens(c, pairIdx){
  const b=((c&&c.data&&c.data.badges)||[])[pairIdx];
  const flat=(b&&b.flat)||{};
  const add=(b&&b.add)||{};
  const link=(((c&&c.data&&c.data.reward_link)||[])[pairIdx])||{};
  return searchFieldTokens([flat.REWARD_CODE,flat.FULL_NAME,flat.REWARD_DESCRIPTION,flat.REWARD_TYPE,add.businessBlock,add.itemAmount,link.REWARD_CODE]);
}
function visibleItemPairIndexes(c){
  if(!contestListRewardTypes.has("ITEM")) return [];
  const all=contestItemPairIndexes(c).filter(idx=>itemMatchesBusinessBlockFilter(c,idx));
  if(!all.length) return [];
  const needle=String(contestListQuery||"").trim().toLowerCase();
  if(!needle) return all;
  const hits=all.filter(idx=>tokensMatchQuery(itemListTokens(c,idx), needle));
  return hits.length?hits:all;
}
function itemFocusActive(){
  if(activePairFocus==null||activeArchiveId) return false;
  const b=((data().badges)||[])[activePairFocus];
  return isItemBadge(b);
}
/** Пары в шапке и во вкладках workspace: ITEM только выбранный, остальные типы как раньше. */
function navVisiblePairIndexes(){
  const c=cur();
  const n=((c&&c.data&&c.data.badges)||[]).length;
  const itemSet=new Set(contestItemPairIndexes(c));
  if(!itemSet.size) return Array.from({length:n},(_,i)=>i);
  const nonItem=Array.from({length:n},(_,i)=>i).filter(i=>!itemSet.has(i));
  if(itemFocusActive()) return [activePairFocus].concat(nonItem.filter(i=>i!==activePairFocus));
  return nonItem;
}
function clampActivePairFocus(){
  if(activePairFocus==null) return;
  const n=((data().badges)||[]).length;
  if(!n||activePairFocus<0||activePairFocus>=n||!isItemBadge(data().badges[activePairFocus])){
    activePairFocus=null;
  }
}
function contestRewardsAreEmpty(c){
  const badges=c&&c.data?c.data.badges:undefined;
  if(!Array.isArray(badges)||!badges.length) return true;
  return contestRewardTypeCodes(c).length===0;
}
function contestMatchesRewardTypeFilter(c){
  if(!contestListRewardTypes.size) return false;
  if(contestRewardsAreEmpty(c)) return contestListRewardTypes.has(FILTER_EMPTY);
  return contestRewardTypeCodes(c).some(t=>contestListRewardTypes.has(t));
}
function contestMatchesDateFilter(c){
  const ymd=String(contestListDate||"").trim();
  if(!isIsoDate(ymd)) return true;
  return contestScheduleRows(c).some(row=>{
    const start=String((row&&row.START_DT)||"").trim();
    const end=String((row&&row.END_DT)||"").trim();
    if(!isIsoDate(start)||!isIsoDate(end)) return false;
    return start<=ymd && ymd<=end;
  });
}
function contestMatchesLiveFilters(c){
  return contestMatchesStandFilter(c)&&contestMatchesEnvFilter(c)&&contestMatchesBusinessBlockFilter(c)&&contestMatchesStatusFilter(c)&&contestMatchesDateFilter(c)&&contestMatchesRewardTypeFilter(c);
}
function visibleContestEntries(){
  const out=[];
  contests.forEach((c,i)=>{
    if(!contestMatchesKindFilter(c)) return;
    if(!contestMatchesListQuery(c,contestListQuery)) return;
    if(!contestMatchesLiveFilters(c)) return;
    out.push({c,i});
  });
  return out;
}
function pruneSelectedContestIds(){
  const live=new Set(contests.map(c=>c.id));
  for(const id of [...selectedContestIds]){
    if(!live.has(id)) selectedContestIds.delete(id);
  }
}
function exportSelectionState(){
  pruneSelectedContestIds();
  const total=contests.length;
  const picked=selectedContestIds.size;
  if(!contestSelectMode) return {mode:"all", list:contests, picked:0, total};
  if(!picked) return {mode:"empty", list:null, picked:0, total};
  if(picked>=total) return {mode:"all", list:contests, picked, total};
  return {mode:"partial", list:contests.filter(c=>selectedContestIds.has(c.id)), picked, total};
}
function refreshExportButtonLabels(){
  const st=exportSelectionState();
  const all=st.mode==="all"||(!contestSelectMode);
  const jsonMain=$("btn-save-project-label");
  const jsonSub=$("btn-save-project-sub");
  const csvMain=$("btn-export-all-label");
  const csvSub=$("btn-export-all-sub");
  if(jsonMain) jsonMain.textContent=(!contestSelectMode||all)?"Скачать все JSON":"Скачать выбранные JSON";
  if(csvMain) csvMain.textContent=(!contestSelectMode||all)?"Скачать все CSV":"Скачать выбранные CSV";
  if(jsonSub) jsonSub.textContent=contestSelectMode?(all&&st.picked?`все ${st.total}`:`выбрано ${st.picked} из ${st.total}`):(st.total?`все ${st.total}`:"");
  if(csvSub) csvSub.textContent=contestSelectMode?(all&&st.picked?`все ${st.total}`:`выбрано ${st.picked} из ${st.total}`):(st.total?`все ${st.total}`:"");
  const modeBtn=$("btn-select-mode");
  if(modeBtn){
    modeBtn.classList.toggle("is-on", contestSelectMode);
    modeBtn.setAttribute("aria-pressed", contestSelectMode?"true":"false");
  }
  const allBtn=$("btn-select-all");
  const noneBtn=$("btn-select-none");
  if(allBtn) allBtn.hidden=!contestSelectMode;
  if(noneBtn) noneBtn.hidden=!contestSelectMode;
}
function setContestSelectMode(on){
  contestSelectMode=!!on;
  refreshExportButtonLabels();
  renderContestTabs();
}
function selectVisibleContests(){
  visibleContestEntries().forEach(({c})=>{if(c&&c.id) selectedContestIds.add(c.id)});
  refreshExportButtonLabels();
  renderContestTabs();
}
function clearSelectedContests(){
  selectedContestIds.clear();
  refreshExportButtonLabels();
  renderContestTabs();
}
function syncFilterBtn(el, on){
  if(!el) return;
  el.classList.toggle("is-on",!!on);
  el.setAttribute("aria-pressed",on?"true":"false");
}
function rewardTypeFilterOptions(){
  const f=typeof rewardTypeField==="function"?rewardTypeField():null;
  const variants=(f&&Array.isArray(f.variants)&&f.variants.length)?f.variants.map(v=>String(v)):REWARD_TYPE_FILTER_FALLBACK.slice();
  return variants.map(code=>{
    const lab=labelForVariant(f||{}, code);
    return {code, label:lab&&lab!==code?lab:code};
  });
}
function ensureRewardTypeFilterButtons(){
  const host=$("filter-reward-types");
  if(!host) return;
  if(host.dataset.ready==="1") return;
  const byCode={};
  rewardTypeFilterOptions().forEach(o=>{byCode[o.code]=o});
  function chipHtml(code){
    const empty=code===FILTER_EMPTY;
    const o=byCode[code]||{code,label:empty?REWARD_TYPE_EMPTY_LABEL:code};
    const label=empty?REWARD_TYPE_EMPTY_LABEL:(o.label||code);
    const cls="contest-kind-btn contest-kind-btn--rtype"+(empty?" contest-kind-btn--rtype-empty":"");
    const tip=empty
      ?"Нет наград: массив badges пуст / ключа нет, либо REWARD_TYPE пустой"
      :"Показать конкурсы, где есть награда "+label+" ("+code+")";
    return `<button type="button" class="${cls} is-on" data-reward-type="${esc(code)}" aria-pressed="true" data-tip="${esc(tip)}">${esc(label)}</button>`;
  }
  const rows=REWARD_TYPE_FILTER_LAYOUT.map(row=>row.slice());
  rewardTypeFilterOptions().forEach(o=>{
    if(o.code===FILTER_EMPTY) return;
    if(!rows.some(r=>r.includes(o.code))) rows[0].push(o.code);
  });
  if(!rows.some(r=>r.includes(FILTER_EMPTY))) rows[rows.length-1].push(FILTER_EMPTY);
  contestListRewardTypes=new Set(rows.flat());
  host.innerHTML=rows.map(row=>`<div class="rtype-row" role="group">${row.map(chipHtml).join("")}</div>`).join("");
  host.dataset.ready="1";
  host.querySelectorAll("[data-reward-type]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const t=btn.getAttribute("data-reward-type")||"";
      if(!t) return;
      if(contestListRewardTypes.has(t)) contestListRewardTypes.delete(t);
      else contestListRewardTypes.add(t);
      renderContestTabs();
    });
  });
}
function syncContestKindFilterButtons(){
  ensureRewardTypeFilterButtons();
  syncFilterBtn($("filter-tournaments"),contestListShowTournament);
  syncFilterBtn($("filter-rewards"),contestListShowReward);
  syncFilterBtn($("filter-archive"),contestListShowArchive);
  syncFilterBtn($("filter-env-prom"),contestListShowProm);
  syncFilterBtn($("filter-env-test"),contestListShowTest);
  document.querySelectorAll("[data-stand]").forEach(btn=>{
    const t=btn.getAttribute("data-stand")||"";
    syncFilterBtn(btn, contestListStandFilter.has(t));
  });
  document.querySelectorAll("[data-status]").forEach(btn=>{
    const st=btn.getAttribute("data-status")||"";
    syncFilterBtn(btn, contestListStatuses.has(st));
  });
  document.querySelectorAll("[data-reward-type]").forEach(btn=>{
    const t=btn.getAttribute("data-reward-type")||"";
    syncFilterBtn(btn, contestListRewardTypes.has(t));
  });
  document.querySelectorAll("[data-bb]").forEach(btn=>{
    const t=btn.getAttribute("data-bb")||"";
    syncFilterBtn(btn, contestListBusinessBlocks.has(t));
  });
  document.querySelectorAll("[data-search-mode]").forEach(btn=>{
    syncFilterBtn(btn, (btn.getAttribute("data-search-mode")||"")==contestListSearchMode);
  });
  const clear=$("filter-date-clear");
  if(clear) clear.hidden=!isIsoDate(contestListDate);
}
function collectedFilterCodes(attr){
  return Array.from(document.querySelectorAll("["+attr+"]"))
    .map(btn=>btn.getAttribute(attr)||"")
    .filter(Boolean);
}
function refreshListAfterFilterPreset(){
  remountContestListDateFilter();
  if(!contestListShowArchive&&activeArchiveId){activeArchiveId=null;render();return}
  renderContestTabs();
}
/** Все чипы выкл., дата пустая. Поиск не трогаем. */
function clearAllListFilters(){
  ["type","env","stand","bb","rtype","status"].forEach(clearFilterGroup);
  contestListDate="";
  refreshListAfterFilterPreset();
}
/** Все чипы вкл. (в т.ч. архив и пустые), дату сбрасываем. */
function enableAllListFiltersExceptDate(){
  ["type","env","stand","bb","rtype","status"].forEach(enableFilterGroup);
  contestListDate="";
  refreshListAfterFilterPreset();
}
/** Выключить все чипы одного блока фильтров. */
function clearFilterGroup(group){
  switch(group){
    case "type":
      contestListShowTournament=false;
      contestListShowReward=false;
      contestListShowArchive=false;
      break;
    case "env":
      contestListShowProm=false;
      contestListShowTest=false;
      break;
    case "stand":
      contestListStandFilter=new Set();
      break;
    case "bb":
      contestListBusinessBlocks=new Set();
      break;
    case "rtype":
      ensureRewardTypeFilterButtons();
      contestListRewardTypes=new Set();
      break;
    case "status":
      contestListStatuses=new Set();
      break;
    case "date":
      contestListDate="";
      break;
    default:
      return;
  }
  if(group==="type"&&!contestListShowArchive&&activeArchiveId){activeArchiveId=null;render();return}
  if(group==="date"){remountContestListDateFilter();renderContestTabs();return}
  renderContestTabs();
}
/** Включить все чипы одного блока фильтров. */
function enableFilterGroup(group){
  switch(group){
    case "type":
      contestListShowTournament=true;
      contestListShowReward=true;
      contestListShowArchive=true;
      break;
    case "env":
      contestListShowProm=true;
      contestListShowTest=true;
      break;
    case "stand":
      contestListStandFilter=new Set(collectedFilterCodes("data-stand"));
      if(!contestListStandFilter.size) contestListStandFilter=new Set(STAND_FILTER_CODES);
      break;
    case "bb":
      contestListBusinessBlocks=new Set(collectedFilterCodes("data-bb"));
      break;
    case "rtype":
      ensureRewardTypeFilterButtons();
      contestListRewardTypes=new Set(collectedFilterCodes("data-reward-type"));
      break;
    case "status":
      contestListStatuses=new Set(collectedFilterCodes("data-status"));
      break;
    default:
      return;
  }
  renderContestTabs();
}

function openContestFromList(i, pairIdx){
  activeArchiveId=null;
  activeContest=i;
  closeAllDatePops();
  const c=cur();
  seedLinked(c.data);
  ensureScheduleSorted();
  realignBaselineIfPristine(c);
  if(pairIdx==null){
    activePairFocus=null;
    activeSection="CONTEST";
    activeBadge=0;activeSchedule=0;activeIndicator=0;activeGroup=0;activeLink=0;
  }else{
    activePairFocus=pairIdx;
    activeLink=pairIdx;
    activeBadge=pairIdx;
    activeSection="PAIR:"+(pairIdx+1);
  }
  render();
}
function contestParentTabHtml(c,i){
  const dirty=isContestDirty(c);
  const tone=contestScheduleListTone(c);
  const tip=contestMenuTip(c,i)+(dirty?" · есть несохранённые правки":"")+(tone.label?" · "+tone.label:"");
  const tabTip=dirty?"Есть правки параметров":(tone.label||"Текущий конкурс");
  const on=i===activeContest&&!activeArchiveId;
  const childOn=on&&activePairFocus!=null;
  const selfOn=on&&activePairFocus==null;
  const picked=selectedContestIds.has(c.id);
  const check=contestSelectMode
    ?`<label class="ct-check" data-tip="Включить в выгрузку JSON/CSV"><input type="checkbox" data-pick-id="${esc(c.id)}" ${picked?"checked":""}></label>`
    :"";
  return `<div class="contest-tab ${tone.cls}${selfOn?" active":""}${childOn?" is-parent-on":""}${dirty?" is-dirty":""}${picked?" is-picked":""}" data-ci="${i}" data-tip="${esc(tabTip)}">${check}<span class="ct-text" data-tip="${esc(tip)}"><span class="ct-code">${contestCodeLineHtml(c,i)}${standBadgesHtml(contestItemStands(c),{inline:true,title:"Стенд"})}</span><span class="ct-fullname">${esc(contestNameLine(c))}</span></span>${contests.length>1?`<button type="button" class="ct-x" data-del="${i}" data-tip="В архив">×</button>`:""}</div>`;
}
function contestItemTabHtml(c,i,pairIdx){
  const code=itemListCode(c,pairIdx);
  const name=itemListName(c,pairIdx);
  const amt=itemListAmount(c,pairIdx);
  const tip=(name?code+" · "+name:code)+(amt?" · Ct: "+amt:"");
  const on=i===activeContest&&!activeArchiveId&&activePairFocus===pairIdx;
  return `<div class="contest-tab contest-tab--item${on?" active":""}" data-ci="${i}" data-pair="${pairIdx}" data-tip="${esc(tip)}"><span class="ct-text" data-tip="${esc(tip)}"><span class="ct-code">${itemCodeLineHtml(c,pairIdx)}</span><span class="ct-fullname">${esc(name)}</span></span><button type="button" class="ct-x" data-del-pair="${pairIdx}" data-tip="В архив">×</button></div>`;
}
function renderContestTabs(){
  const host=$("contest-tabs");
  if(!host)return;
  syncContestKindFilterButtons();
  host.classList.toggle("contest-tabs--picking", contestSelectMode);
  const rewards=[];
  const tournaments=[];
  contests.forEach((c,i)=>{
    if(!contestMatchesKindFilter(c)) return;
    if(!contestMatchesListQuery(c,contestListQuery)) return;
    if(!contestMatchesLiveFilters(c)) return;
    if(contestMenuKind(c)==="reward") rewards.push({c,i});
    else tournaments.push({c,i});
  });
  function groupHtml(kind, title, items){
    if(!items.length) return "";
    const itemN=items.reduce((n,{c})=>n+visibleItemPairIndexes(c).length,0);
    const count=items.length+(itemN?` · товаров ${itemN}`:"");
    return `<div class="contest-tabs-group contest-tabs-group--${kind}">`+
      `<div class="contest-tabs-group__title">${esc(title)} · ${count}</div>`+
      items.map(({c,i})=>{
        const kids=visibleItemPairIndexes(c);
        const parent=contestParentTabHtml(c,i);
        if(!kids.length) return parent;
        return `<div class="contest-tab-tree">${parent}${kids.map(j=>contestItemTabHtml(c,i,j)).join("")}</div>`;
      }).join("")+
      `</div>`;
  }
  let html=groupHtml("reward","Награды",rewards)+groupHtml("tournament","Турниры",tournaments);
  if(contestListShowArchive){
    const arch=archiveEntries.filter(e=>archiveMatchesListQuery(e,contestListQuery));
    if(arch.length){
      html+=`<div class="contest-tabs-group contest-tabs-group--archive">`+
        `<div class="contest-tabs-group__title">Архив · ${arch.length}</div>`+
        arch.map(e=>{
          const st=archiveBundleStats(e);
          const tip=(e.label||"")+" · "+(st.whole?"целиком":"части")+" · R:"+st.r+" · T:"+st.t+" · G:"+st.g;
          const line2=String(e.contestName||"").trim();
          return `<div class="contest-tab contest-tab--archive${activeArchiveId===e.id?" active":""}" data-arch="${esc(e.id)}" data-tip="${esc(tip)}"><span class="ct-text" data-tip="${esc(tip)}">${archiveBundleListHtml(e)}${line2?`<span class="ct-fullname">${esc(line2)}</span>`:""}</span></div>`;
        }).join("")+
        `</div>`;
    }
  }
  host.innerHTML=html;
  if(!host.innerHTML.trim()){
    const q=String(contestListQuery||"").trim();
    if(!contestListShowTournament&&!contestListShowReward&&!contestListShowArchive){
      host.innerHTML=`<div class="contest-tabs-empty">Включите «Турниры», «Награды» или «Архив»</div>`;
    }else if(contestListShowArchive&&!archiveEntries.length&&!rewards.length&&!tournaments.length){
      host.innerHTML=`<div class="contest-tabs-empty">Архив пуст</div>`;
    }else if(q){
      host.innerHTML=`<div class="contest-tabs-empty">Нет конкурсов по запросу «${esc(q)}» и текущим фильтрам</div>`;
    }else{
      host.innerHTML=`<div class="contest-tabs-empty">Нет конкурсов по текущим фильтрам</div>`;
    }
  }
  host.querySelectorAll("[data-ci]").forEach(el=>{
    el.addEventListener("click",e=>{
      if(e.target.closest("[data-del]"))return;
      if(e.target.closest("[data-del-pair]"))return;
      if(e.target.closest("[data-pick-id]"))return;
      const i=Number(el.getAttribute("data-ci"));
      const pairRaw=el.getAttribute("data-pair");
      const pairIdx=pairRaw==null?null:Number(pairRaw);
      openContestFromList(i, Number.isFinite(pairIdx)?pairIdx:null);
    });
  });
  host.querySelectorAll("[data-pick-id]").forEach(inp=>{
    inp.addEventListener("click",e=>e.stopPropagation());
    inp.addEventListener("change",e=>{
      e.stopPropagation();
      const id=inp.getAttribute("data-pick-id")||"";
      if(!id) return;
      if(inp.checked) selectedContestIds.add(id);
      else selectedContestIds.delete(id);
      const tab=inp.closest(".contest-tab");
      if(tab) tab.classList.toggle("is-picked", inp.checked);
      refreshExportButtonLabels();
    });
  });
  host.querySelectorAll("[data-del]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      const i=Number(btn.getAttribute("data-del"));
      if(!confirm("Переместить конкурс в архив?")) return;
      archiveContestAt(i);
    });
  });
  host.querySelectorAll("[data-del-pair]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      const wrap=btn.closest("[data-ci]");
      const i=Number(wrap&&wrap.getAttribute("data-ci"));
      const j=Number(btn.getAttribute("data-del-pair"));
      if(!Number.isFinite(i)||!Number.isFinite(j)) return;
      if(activeContest!==i){
        activeArchiveId=null;
        activeContest=i;
        seedLinked(cur().data);
        ensureScheduleSorted();
      }
      if(deletePairAt(j)) render();
    });
  });
  host.querySelectorAll("[data-arch]").forEach(el=>{
    el.addEventListener("click",()=>{
      activeArchiveId=el.getAttribute("data-arch");
      activePairFocus=null;
      const entry=findArchiveEntry(activeArchiveId);
      activeArchiveSection=defaultArchiveSection(entry);
      activeSection="ARCHIVE";
      closeAllDatePops();
      render();
    });
  });
  refreshExportButtonLabels();
}

function navItems(){
  ensureJsonStructures(data());
  const n=data().badges.length;
  const max=maxBadges(data().contest.CONTEST_TYPE);
  const groupRows=data().group||[];
  const indRows=data().indicator||[];

  // Строка 1: Конкурс → Индикаторы → Группы
  const rowMain=[];
  rowMain.push({kind:"group",group:"contest",title:"Конкурс",meta:contestGroupMetaLabel(data().contest.CONTEST_TYPE)});
  {
    const title=contestNavButtonTitle();
    const sub=contestCardSub();
    const typeLab=contestTypeNavLabel(data().contest.CONTEST_TYPE);
    rowMain.push({
      id:"CONTEST",
      title,
      sub,
      tag:"start",
      tagLabel:"DATA",
      slot:true,
      nameSlot:true,
      tip:(sub?title+" · "+sub:title)+(typeLab?" · "+typeLab:""),
    });
  }
  {
    const periodNav=contestPeriodNavItem();
    rowMain.push({
      id:"CONTEST_FEATURE",
      title:"Особенности",
      sub:"CONTEST_FEATURE",
      tag:"json",
      tagLabel:"JSON",
      slot:true,
      child:true,
      tip:"Особенности · CONTEST_FEATURE",
    });
    if(periodNav) rowMain.push(periodNav);
  }

  rowMain.push({kind:"group",group:"ind",title:"Индикаторы",meta:indRows.length+" шт."});
  if(!indRows.length){
    rowMain.push({id:"INDICATOR",title:"Нет индикаторов",sub:"добавить",tag:"table",tagLabel:"I",slot:true,tip:"В снимке нет строк INDICATOR — добавить вручную"});
  }else{
    indRows.forEach((r,i)=>{
      const title=indicatorTitle(r,i);
      const sub=indicatorSub(r);
      const fc=indicatorFilterCount(r);
      rowMain.push({
        id:"INDICATOR:"+(i+1),
        title,
        sub,
        tag:"table",
        tagLabel:"I"+(i+1),
        slot:true,
        tip:(sub?title+" · "+sub:title)+(fc?(" · фильтры F×"+fc):""),
        canDelete:true,
        delKind:"indicator",
        delIndex:i,
      });
      const filt=indicatorFilterNavItem(r,i);
      if(filt) rowMain.push(filt);
    });
  }

  rowMain.push({kind:"group",group:"groups",title:"Группы",meta:groupRows.length+" шт."});
  if(!groupRows.length){
    rowMain.push({id:"GROUP",title:"Нет групп",sub:"добавить",tag:"table",tagLabel:"G",slot:true,tip:"В снимке нет строк GROUP — добавить вручную"});
  }else{
    groupRows.forEach((r,i)=>{
      const title=groupTitle(r,i);
      const sub=groupSub(r);
      rowMain.push({
        id:"GROUP:"+(i+1),
        title,
        sub,
        tag:"table",
        tagLabel:"G"+(i+1),
        slot:true,
        tip:sub?title+" · "+sub:title,
        canDelete:true,
        delKind:"group",
        delIndex:i,
      });
    });
  }

  // Строка 2: Связи + награды (+ особенности JSON). ITEM — только выбранный в списке.
  const rowLinks=[];
  const pairIdxs=navVisiblePairIndexes();
  const itemN=contestItemPairIndexes(cur()).length;
  let pairMeta=n+(n>max?" · рек. "+max:" / рек. "+max);
  if(itemN){
    pairMeta=itemFocusActive()
      ?"товар "+(contestItemPairIndexes(cur()).indexOf(activePairFocus)+1)+" из "+itemN
      :(itemN+" товаров · выберите слева");
    if(pairIdxs.length&&pairIdxs.some(i=>!isItemBadge(data().badges[i]))) pairMeta+=" · ещё "+pairIdxs.filter(i=>!isItemBadge(data().badges[i])).length;
  }
  rowLinks.push({kind:"group",group:"pair",title:"Связи + награды",meta:pairMeta});
  if(!n){
    rowLinks.push({id:"PAIR",title:"Нет связей",sub:"добавить",tag:"table",tagLabel:"R",slot:true,tip:"В снимке нет пар связь+награда — добавить вручную"});
  }else if(!pairIdxs.length){
    rowLinks.push({id:"CONTEST",title:"Выберите товар",sub:"в списке слева · "+itemN+" шт.",tag:"table",tagLabel:"ITEM",slot:true,tip:"У конкурса много товаров (ITEM). Выберите строку под конкурсом — в шапке останется только этот reward"});
  }else{
  for(const i of pairIdxs){
    const label=pairNavTitle(i);
    const code=badgeRewardCode(i)||"";
    rowLinks.push({
      kind:"pairUnit",
      index:i,
      pairId:"PAIR:"+(i+1),
      addId:"ADD:"+(i+1),
      pairTitle:label,
      pairSub:pairTypeSub(data().badges[i]),
      addTitle:"Особенности",
      addSub:code?("JSON · "+code):"REWARD_ADD_DATA",
      tipPair:label+" · "+pairTypeSub(data().badges[i]),
      tipAdd:"JSON REWARD_ADD_DATA для награды "+(code||("#"+(i+1))),
    });
  }
  }

  // Строка 3: Расписание (всегда последняя) — порядок по TOURNAMENT_STATUS
  ensureScheduleSorted();
  const schSorted=data().schedule||[];
  const rowSchedule=[];
  rowSchedule.push({kind:"group",group:"sch",title:"Расписание",meta:schSorted.length+" тур."});
  if(!schSorted.length){
    rowSchedule.push({id:"SCHEDULE",title:"Нет турниров",sub:"добавить",tag:"table",tagLabel:"T",slot:true,tip:"В снимке нет строк SCHEDULE — добавить вручную"});
  }else{
    schSorted.forEach((r,i)=>{
      const title=scheduleTitle(r,i);
      const sub=scheduleSub(r);
      const badges=scheduleJsonBadges(r);
      rowSchedule.push({
        id:"SCHEDULE:"+(i+1),
        title,
        sub,
        tag:"table",
        tagLabel:"T"+(i+1),
        slot:true,
        codeSlot:true,
        statusClass:scheduleStatusNavClass(r.TOURNAMENT_STATUS),
        badges,
        tip:(sub?title+" · "+sub:title)+(badges.length?" · "+badges.map(b=>b.text).join(" · "):""),
        canDelete:true,
        delKind:"schedule",
        delIndex:i,
      });
    });
  }

  return {rowMain,rowLinks,rowSchedule};
}

function badgeRewardCode(i){
  const b=(data().badges||[])[i];
  return String((b&&b.flat&&b.flat.REWARD_CODE)||"").trim();
}
function badgeNavTitle(i){
  const code=badgeRewardCode(i);
  return code||("Награда "+(i+1));
}
function linkNavTitle(i){
  const row=((data().reward_link)||[])[i]||{};
  const rc=String(row.REWARD_CODE||"").trim();
  const gc=String(row.GROUP_CODE||"").trim();
  if(rc) return rc+(gc?" · "+gc:"");
  if(gc) return "Связь · "+gc;
  return "Связь "+(i+1);
}
function pairNavTitle(i){
  return badgeRewardCode(i)||linkNavTitle(i)||("Пара "+(i+1));
}
function rewardTypeField(){
  return ((catalog.sections.find(s=>s.id==="REWARD")||{fields:[]}).fields||[]).find(f=>f.key==="REWARD_TYPE")||null;
}
function rewardTypeCode(badge){
  const t=String((badge&&badge.flat&&badge.flat.REWARD_TYPE)||"").trim();
  return t||"BADGE";
}
function rewardTypeFace(code){
  const c=String(code||"").trim()||"BADGE";
  const lab=labelForVariant(rewardTypeField()||{}, c);
  return lab&&lab!==c?lab+" · "+c:c;
}
function pairTypeSub(badge){
  return "связь + "+rewardTypeFace(rewardTypeCode(badge));
}
function pairRewardHint(i, badge){
  const rc=badgeRewardCode(i)||"";
  return (rc||"код из связи выше")+" · "+rewardTypeFace(rewardTypeCode(badge));
}
function refreshPairTypeChrome(i, badge){
  const face=rewardTypeFace(rewardTypeCode(badge));
  const hint=$("workspace")?.querySelector(".pair-block--reward .pair-block__hint");
  if(hint) hint.textContent=pairRewardHint(i, badge);
  const pill=$("workspace")?.querySelector('.ctx-pill[data-k="TYPE"] .ctx-pill__v');
  if(pill) pill.textContent=face;
  const tipEl=$("workspace")?.querySelector('.ctx-pill[data-k="TYPE"]');
  if(tipEl) tipEl.setAttribute("data-tip","Тип награды (REWARD_TYPE): "+face);
}
function clampIndex(i,len){
  if(!len) return 0;
  if(!Number.isFinite(i)||i<0) return 0;
  if(i>=len) return len-1;
  return i;
}
function normalizeActiveSection(){
  clampActivePairFocus();
  if(activeSection==="REWARD-LINK"||activeSection==="BADGE"||activeSection==="LINK"){
    activeSection=(data().badges||[]).length?"PAIR:"+(activeLink+1):"PAIR";
  }
  if(activeSection==="GROUP"&&(data().group||[]).length) activeSection="GROUP:"+(activeGroup+1);
  if(activeSection==="INDICATOR"&&(data().indicator||[]).length) activeSection="INDICATOR:"+(activeIndicator+1);
  if(activeSection==="SCHEDULE"&&(data().schedule||[]).length) activeSection="SCHEDULE:"+(activeSchedule+1);
  if(activeSection.startsWith("ADD:")){
    const n=(data().badges||[]).length;
    if(!n){activeSection="PAIR";return}
    let i=Number(activeSection.split(":")[1])-1;
    i=clampIndex(i,n);
    activeLink=i;activeBadge=i;
    activeSection="ADD:"+(i+1);
    if(isItemBadge((data().badges||[])[i])) activePairFocus=i;
    else activePairFocus=null;
    return;
  }
  if(activeSection.startsWith("BADGE:")||activeSection.startsWith("LINK:")||activeSection.startsWith("PAIR:")){
    const n=(data().badges||[]).length;
    if(!n){activeSection="PAIR";activePairFocus=null;return}
    let i=Number(activeSection.split(":")[1])-1;
    i=clampIndex(i,n);
    activeLink=i;activeBadge=i;
    activeSection="PAIR:"+(i+1);
    if(isItemBadge((data().badges||[])[i])) activePairFocus=i;
    else activePairFocus=null;
  }
  if(activeSection.startsWith("GROUP:")){
    const n=(data().group||[]).length;
    if(!n){activeSection="GROUP";return}
    let i=Number(activeSection.split(":")[1])-1;
    i=clampIndex(i,n);
    activeGroup=i;
    activeSection="GROUP:"+(i+1);
  }
  if(activeSection.startsWith("INDICATOR:")){
    const n=(data().indicator||[]).length;
    if(!n){activeSection="INDICATOR";return}
    let i=Number(activeSection.split(":")[1])-1;
    i=clampIndex(i,n);
    activeIndicator=i;
    activeSection="INDICATOR:"+(i+1);
  }
  if(activeSection.startsWith("SCHEDULE:")){
    const n=(data().schedule||[]).length;
    if(!n){activeSection="SCHEDULE";return}
    let i=Number(activeSection.split(":")[1])-1;
    i=clampIndex(i,n);
    activeSchedule=i;
    activeSection="SCHEDULE:"+(i+1);
  }
}

function navBtnHtml(it){
  const child=it.child?" nav-btn--child":"";
  const compact=it.compact?" nav-btn--compact":"";
  const slot=(it.slot||it.child)?" nav-btn--slot":"";
  const json=it.tag==="json"?" nav-btn--json":"";
  const code=it.codeSlot?" nav-btn--code":"";
  const name=it.nameSlot?" nav-btn--name":"";
  const status=it.statusClass?(" "+it.statusClass):"";
  const del=it.canDelete?`<span class="nav-btn__x" role="button" tabindex="0" data-nav-del="${esc(it.delKind)}" data-nav-del-i="${it.delIndex}" data-tip="В архив">×</span>`:"";
  const sub=it.sub?`<span class="nav-sub">${esc(it.sub)}</span>`:"";
  const badges=(it.badges||[]).map(b=>`<span class="nav-json-badge${b.kind?" nav-json-badge--"+esc(b.kind):""}" data-tip="${esc(b.tip||b.text)}">${esc(b.text)}</span>`).join("");
  const on=it.id==="CONTEST_PERIOD"?periodNavFocus:(activeSection===it.id&&!(it.id==="CONTEST"&&periodNavFocus));
  return `<button type="button" class="nav-btn${slot}${child}${compact}${json}${code}${name}${status}${on?" active":""}" data-nav="${esc(it.id)}" data-tip="${esc(it.tip||it.title)}"><span class="nav-btn__body"><span class="nav-btn__title">${esc(it.title)}</span>${sub}</span>${badges}<span class="tag ${it.tag}">${esc(it.tagLabel)}</span>${del}</button>`;
}

function renderNavRow(raw){
  let html="";
  let i=0;
  while(i<raw.length){
    const it=raw[i];
    if(it.kind==="group"){
      const cluster=it.group;
      let j=i+1;
      const kids=[];
      while(j<raw.length && (raw[j].slot||raw[j].kind==="pairUnit")){
        kids.push(raw[j]);
        j++;
      }
      const schSlots=cluster==="sch"?kids.filter(k=>k.kind!=="pairUnit"&&!k.compact).length:0;
      const schMulti=schSlots>6;
      const schPerRow=schMulti?Math.ceil(schSlots/2):0;
      const multiCls=schMulti?" nav-group--sch-multi":"";
      const multiStyle=schMulti?` style="--sch-per-row:${schPerRow}"`:"";
      html+=`<div class="nav-group nav-group--${esc(cluster)}${multiCls}"${multiStyle} role="group" aria-label="${esc(it.title)}">`;
      html+=`<div class="nav-group__title"><span>${esc(it.title)}</span><span class="nav-group__meta">${esc(it.meta||"")}</span></div>`;
      for(const s of kids){
        if(s.kind==="pairUnit"){
          const pairOn=activeSection===s.pairId;
          const addOn=activeSection===s.addId;
          const unitCls=addOn?" is-on-add":(pairOn?" is-on":"");
          html+=`<div class="nav-pair-unit${unitCls}" data-pair-unit="${s.index}">`;
          html+=`<span class="nav-pair-unit__n" aria-hidden="true">${s.index+1}</span>`;
          html+=`<button type="button" class="nav-btn nav-btn--slot${pairOn?" active":""}" data-nav="${esc(s.pairId)}" data-tip="${esc(s.tipPair)}"><span class="nav-btn__body"><span class="nav-btn__title">${esc(s.pairTitle)}</span>${s.pairSub?`<span class="nav-sub">${esc(s.pairSub)}</span>`:""}</span><span class="tag table">R${s.index+1}</span></button>`;
          html+=`<button type="button" class="nav-btn nav-btn--slot nav-btn--json nav-btn--child${addOn?" active":""}" data-nav="${esc(s.addId)}" data-tip="${esc(s.tipAdd)}"><span class="nav-btn__body"><span class="nav-btn__title">${esc(s.addTitle)}</span>${s.addSub?`<span class="nav-sub">${esc(s.addSub)}</span>`:""}</span><span class="tag json">JSON</span></button>`;
          if(!s.hideDelete) html+=`<button type="button" class="nav-pair-unit__x" data-nav-del="pair" data-nav-del-i="${s.index}" data-tip="В архив">×</button>`;
          html+=`</div>`;
        }else{
          html+=navBtnHtml(s);
        }
      }
      html+=`</div>`;
      i=j;
      continue;
    }
    html+=navBtnHtml(it);
    i++;
  }
  return html;
}

function renderNav(){
  normalizeActiveSection();
  const nav=$("section-nav");
  if(!nav)return;
  if(!sessionReady){nav.hidden=true;nav.innerHTML="";return}
  nav.hidden=false;
  const {rowMain,rowLinks,rowSchedule}=navItems();
  const groupN=Math.max((data().group||[]).length,0);
  const mainHeavy=groupN>3?" top-nav__row--groups-heavy":"";
  // 1) Конкурс · Индикаторы · Группы  2) Связи + особенности  3) Расписание
  nav.innerHTML=
    `<div class="top-nav__row top-nav__row--main${mainHeavy}">${renderNavRow(rowMain)}</div>`+
    `<div class="top-nav__row top-nav__row--links">${renderNavRow(rowLinks)}</div>`+
    `<div class="top-nav__row top-nav__row--schedule">${renderNavRow(rowSchedule)}</div>`;
  nav.querySelectorAll("[data-nav-del]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.preventDefault();
      e.stopPropagation();
      handleNavDelete(btn.getAttribute("data-nav-del"), Number(btn.getAttribute("data-nav-del-i")));
    });
  });
  nav.querySelectorAll("[data-nav]").forEach(btn=>btn.addEventListener("click",e=>{
    if(e.target.closest("[data-nav-del]")) return;
    activeSection=btn.getAttribute("data-nav");
    if(activeSection==="CONTEST_PERIOD"){
      periodNavFocus=true;
      pendingScrollTarget="contest-period-block";
      activeSection="CONTEST";
    }else{
      periodNavFocus=false;
    }
    if(activeSection.startsWith("PAIR:")||activeSection.startsWith("ADD:")){
      const idx=Number(activeSection.split(":")[1])-1;
      activeLink=idx;activeBadge=idx;
    }else if(activeSection.startsWith("GROUP:")){
      activeGroup=Number(activeSection.split(":")[1])-1;
    }else if(activeSection.startsWith("INDICATOR:")){
      activeIndicator=Number(activeSection.split(":")[1])-1;
    }else if(activeSection.startsWith("SCHEDULE:")){
      activeSchedule=Number(activeSection.split(":")[1])-1;
    }
    closeAllDatePops();
    render();
  }));
}

