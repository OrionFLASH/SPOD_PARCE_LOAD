/* game_fill_model.js — данные конкурса, коды r_/t_/ITEM_, копирование, архив */
"use strict";
function emptyBadge(){const flat={},add={};for(const f of (catalog.sections.find(s=>s.id==="REWARD")||{fields:[]}).fields){if(f.kind==="json")continue;flat[f.key]=f.default!=null?String(f.default):""}for(const f of (catalog.sections.find(s=>s.id==="REWARD_ADD_DATA")||{fields:[]}).fields){const leaf=jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD");add[leaf]=f.default!=null?String(f.default):""}if(!flat.REWARD_TYPE)flat.REWARD_TYPE="BADGE";return tagNewEntityStands({flat,add})}

function emptyContestData(){
  const contest={},feature={};
  for(const f of (catalog.sections.find(s=>s.id==="CONTEST")||{fields:[]}).fields){
    if(f.kind==="json") continue;
    contest[f.key]=f.default!=null?String(f.default):"";
  }
  for(const f of (catalog.sections.find(s=>s.id==="CONTEST_FEATURE")||{fields:[]}).fields){const leaf=jsonStoreLeaf(f,"CONTEST_FEATURE","FEATURE");feature[leaf]=f.default!=null?String(f.default):""}
  if(!contest.CONTEST_TYPE)contest.CONTEST_TYPE="ТУРНИРНЫЙ";
  const n=maxBadges(contest.CONTEST_TYPE);
  const badges=[];for(let i=0;i<n;i++)badges.push(emptyBadge());
  const blankRow=cols=>Object.fromEntries(cols.map(c=>[c,""]));
  const withDefaults=(sectionId,cols)=>{
    const row=blankRow(cols);
    for(const f of (catalog.sections.find(s=>s.id===sectionId)||{fields:[]}).fields){
      if(f.kind==="json") continue;
      if(f.default!=null&&String(f.default)!==""&&row[f.key]!==undefined) row[f.key]=String(f.default);
    }
    return row;
  };
  const reward_link=[];for(let i=0;i<n;i++)reward_link.push(blankRow(LINK_COLS));
  const data={contest,feature,contestPeriod:[],badges,reward_link,group:[withDefaults("TABLE:GROUP",GROUP_COLS)],indicator:[withDefaults("TABLE:INDICATOR",IND_COLS)],schedule:[withDefaults("TABLE:SCHEDULE",SCH_COLS)]};
  ensureJsonStructures(data);
  seedLinked(data);
  syncBusinessBlockFromContest(data);
  tagNewEntityStands(data.contest);
  data.group.forEach(tagNewEntityStands);
  data.indicator.forEach(tagNewEntityStands);
  data.schedule.forEach(tagNewEntityStands);
  data.reward_link.forEach(tagNewEntityStands);
  data.badges.forEach(tagNewEntityStands);
  return data;
}


function emptyScheduleRow(){
  const row=Object.fromEntries(SCH_COLS.map(c=>[c,""]));
  const code=contestCodeOf();
  row.CONTEST_CODE=code;
  const suf="P"+((data().schedule||[]).length+1);
  row.TOURNAMENT_CODE=buildPrefixedCode(tournamentCodePrefix(code), suf);
  row.TOURNAMENT_CODE_ENDING=normalizeCodeSuffix(suf);
  for(const f of (catalog.sections.find(s=>s.id==="TABLE:SCHEDULE")||{fields:[]}).fields){
    if(f.key==="TOURNAMENT_CODE"||f.kind==="json") continue;
    if(f.default!=null&&String(f.default)!==""&&!row[f.key]) row[f.key]=String(f.default);
  }
  row.filter_period=[];
  const sf=(catalog.sections.find(s=>s.id==="SCHEDULE_TARGET_TYPE")||{fields:[]}).fields[0];
  row.seasonCode=sf&&sf.default!=null?String(sf.default):"";
  delete row.TARGET_TYPE;
  delete row.FILTER_PERIOD_ARR;
  return tagNewEntityStands(row);
}
function scheduleTitle(row,i){
  const code=String(row.TOURNAMENT_CODE||"").trim();
  if(code) return code;
  return "Период "+(i+1);
}
function scheduleSub(row){
  const parts=[];
  const st=String(row.TOURNAMENT_STATUS||"").trim();
  const pt=String(row.PERIOD_TYPE||"").trim();
  if(st) parts.push(st);
  if(pt) parts.push(pt);
  return parts.join(" · ");
}
/** Число элементов CONTEST_PERIOD. */
function contestPeriodCount(d){
  d=d||data();
  if(!d||typeof d!=="object") return 0;
  ensureJsonStructures(d);
  return Array.isArray(d.contestPeriod)?d.contestPeriod.length:0;
}
/** Компактный признак периодов рядом с FEATURE: наличие + число элементов. */
function contestPeriodNavItem(d){
  const n=contestPeriodCount(d);
  if(n<=0) return null;
  return {
    id:"CONTEST_PERIOD",
    title:"P×"+n,
    sub:"",
    tag:"json",
    tagLabel:"ARR",
    slot:true,
    child:true,
    compact:true,
    tip:"CONTEST_PERIOD · "+n+" "+(n===1?"элемент":(n>=2&&n<=4?"элемента":"элементов")),
  };
}
/** Число элементов FILTER_PERIOD_ARR (наборы в массиве). */
function scheduleFilterPeriodCount(row){
  if(!row||typeof row!=="object") return 0;
  ensureScheduleJson(row);
  return Array.isArray(row.filter_period)?row.filter_period.length:0;
}
function scheduleHasSeasonCode(row){
  if(!row||typeof row!=="object") return false;
  ensureScheduleJson(row);
  return String(row.seasonCode||"").trim()!=="";
}
/** Бейджи JSON на кнопке турнира (видимы в шапке). */
function scheduleJsonBadges(row){
  const badges=[];
  const n=scheduleFilterPeriodCount(row);
  if(n>0){
    badges.push({
      text:"P×"+n,
      kind:"arr",
      tip:"FILTER_PERIOD_ARR · "+n+" "+(n===1?"элемент":(n>=2&&n<=4?"элемента":"элементов")),
    });
  }
  if(scheduleHasSeasonCode(row)){
    const sc=String(row.seasonCode||"").trim();
    badges.push({text:"SC",kind:"obj",tip:"TARGET_TYPE · seasonCode: "+sc});
  }
  return badges;
}
/** Компактные чипы рядом с турниром (дубль бейджей для навигации). */
function scheduleJsonNavItems(row, i, navId){
  const id=navId||("SCHEDULE:"+(i+1));
  return scheduleJsonBadges(row).map(b=>({
    id,
    title:b.text,
    sub:"",
    tag:"json",
    tagLabel:b.kind==="arr"?"ARR":"JSON",
    slot:true,
    child:true,
    compact:true,
    tip:b.tip,
  }));
}

/** Порядок статусов турнира в верхней навигации / списке периодов. */
var SCHEDULE_STATUS_RANK={
  "АКТИВНЫЙ":0,
  "ПОДВЕДЕНИЕ ИТОГОВ":1,
  "ЗАВЕРШЕН":2,
  "ОТМЕНЕН":3,
  "УДАЛЕН":4,
};
function scheduleStatusRank(status){
  const u=String(status||"").trim().toUpperCase();
  return Object.prototype.hasOwnProperty.call(SCHEDULE_STATUS_RANK,u)?SCHEDULE_STATUS_RANK[u]:90;
}
function scheduleStatusNavClass(status){
  const u=String(status||"").trim().toUpperCase();
  if(u==="АКТИВНЫЙ") return "nav-btn--sch-active";
  if(u==="ПОДВЕДЕНИЕ ИТОГОВ") return "nav-btn--sch-results";
  if(u==="ЗАВЕРШЕН") return "nav-btn--sch-done";
  if(u==="ОТМЕНЕН") return "nav-btn--sch-cancel";
  if(u==="УДАЛЕН") return "nav-btn--sch-deleted";
  return "";
}
/**
 * Тон строки конкурса в левом списке по статусам турниров в SCHEDULE.
 * Приоритет: АКТИВНЫЙ → ПОДВЕДЕНИЕ ИТОГОВ → ЗАВЕРШЕН → только ОТМЕНЕН/УДАЛЕН → нет турниров.
 */
function contestScheduleListTone(c){
  const rows=(c&&c.data&&c.data.schedule)||[];
  const statuses=rows
    .map(r=>String((r&&r.TOURNAMENT_STATUS)||"").trim().toUpperCase())
    .filter(Boolean);
  if(!statuses.length) return {cls:"contest-tab--sch-empty",label:"нет турниров в расписании"};
  if(statuses.some(s=>s==="АКТИВНЫЙ")) return {cls:"contest-tab--sch-active",label:"есть активный турнир"};
  if(statuses.some(s=>s==="ПОДВЕДЕНИЕ ИТОГОВ")) return {cls:"contest-tab--sch-results",label:"есть турнир на подведении итогов"};
  if(statuses.some(s=>s==="ЗАВЕРШЕН")) return {cls:"contest-tab--sch-done",label:"есть завершённый турнир"};
  const onlyDead=statuses.every(s=>s==="ОТМЕНЕН"||s==="УДАЛЕН");
  if(onlyDead) return {cls:"contest-tab--sch-cancel",label:"только отменённые / удалённые турниры"};
  return {cls:"contest-tab--sch-empty",label:"нет турниров в ключевых статусах"};
}
/** Сортировка периодов по TOURNAMENT_STATUS; сохраняет активный период. */
function sortScheduleRows(rows){
  if(!rows||rows.length<2) return;
  rows.sort((a,b)=>{
    const d=scheduleStatusRank(a.TOURNAMENT_STATUS)-scheduleStatusRank(b.TOURNAMENT_STATUS);
    if(d) return d;
    return String(a.TOURNAMENT_CODE||"").localeCompare(String(b.TOURNAMENT_CODE||""),"ru");
  });
}
function ensureScheduleSorted(){
  const rows=data().schedule;
  if(!rows||rows.length<2) return;
  const curRow=rows[activeSchedule];
  sortScheduleRows(rows);
  if(curRow){
    const ni=rows.indexOf(curRow);
    if(ni>=0){
      activeSchedule=ni;
      if(String(activeSection||"").startsWith("SCHEDULE:")) activeSection="SCHEDULE:"+(ni+1);
    }
  }
}
/** Нормализация данных конкурса без пометки «правки пользователя». */
function normalizeContestData(d){
  if(!d) return;
  syncBadgeSlots(d,false);
  seedLinked(d);
  sortScheduleRows(d.schedule);
}

function contestTypeNavLabel(t){
  const u=String(t||"").trim().toUpperCase();
  if(u==="ТУРНИРНЫЙ") return "ТУРНИРНЫЙ";
  if(u==="ИНДИВИДУАЛЬНЫЙ") return "ИНДИВИДУАЛЬНЫЙ";
  if(u==="ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ") return "ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ";
  const raw=String(t||"").trim();
  return raw||"Карточка";
}
/** Подпись группы «Конкурс» в шапке: ТУРНИР / НАГРАДА. */
function contestGroupMetaLabel(t){
  const u=String(t||"").trim().toUpperCase();
  if(u==="ТУРНИРНЫЙ") return "ТУРНИР";
  if(u==="ИНДИВИДУАЛЬНЫЙ"||u==="ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ") return "НАГРАДА";
  return "конкурс";
}
function contestCardSub(){
  return String((data().contest&&data().contest.FULL_NAME)||"").trim();
}
/** На кнопке карточки конкурса в шапке — CONTEST_CODE. */
function contestNavButtonTitle(){
  const code=String((data().contest&&data().contest.CONTEST_CODE)||"").trim();
  return code||contestTypeNavLabel(data().contest&&data().contest.CONTEST_TYPE);
}

function emptyIndicatorRow(){
  const row=Object.fromEntries(IND_COLS.map(c=>[c,""]));
  const code=contestCodeOf();
  row.CONTEST_CODE=code;
  const n=(data().indicator||[]).length+1;
  row.N=String(n);
  if(code) row.INDICATOR_CODE=code+"_I"+n;
  for(const f of (catalog.sections.find(s=>s.id==="TABLE:INDICATOR")||{fields:[]}).fields){
    if(f.kind==="json") continue;
    if(f.default!=null&&String(f.default)!==""&&!row[f.key]) row[f.key]=String(f.default);
  }
  row.filter_items=[];
  delete row.INDICATOR_FILTER;
  return tagNewEntityStands(row);
}
function indicatorTitle(row,i){
  const parts=[];
  const code=String(row.INDICATOR_CODE||"").trim();
  const add=String(row.INDICATOR_ADD_CALC_TYPE||"").trim();
  const agg=String(row.INDICATOR_AGG_FUNCTION||"").trim();
  if(code) parts.push(code);
  if(add) parts.push(add);
  if(agg) parts.push(agg);
  if(parts.length) return parts.join(" · ");
  return "Индикатор "+(i+1);
}
function indicatorSub(row){
  const parts=[];
  const mark=String(row.INDICATOR_MARK_TYPE||"").trim();
  const match=String(row.INDICATOR_MATCH||"").trim();
  const val=String(row.INDICATOR_VALUE||"").trim();
  if(mark) parts.push(mark);
  if(match) parts.push(match);
  if(val) parts.push(val);
  return parts.join(" · ");
}
/** Число элементов INDICATOR_FILTER. */
function indicatorFilterCount(row){
  if(!row||typeof row!=="object") return 0;
  ensureIndicatorJson(row);
  return Array.isArray(row.filter_items)?row.filter_items.length:0;
}
/** Компактный чип «есть фильтры» для шапки (как FEATURE/PERIOD, но короче). */
function indicatorFilterNavItem(row, i){
  const n=indicatorFilterCount(row);
  if(n<=0) return null;
  return {
    id:"INDICATOR:"+(i+1),
    title:"F×"+n,
    sub:"",
    tag:"json",
    tagLabel:"ARR",
    slot:true,
    child:true,
    compact:true,
    tip:"INDICATOR_FILTER · "+n+" "+(n===1?"элемент":(n>=2&&n<=4?"элемента":"элементов")),
  };
}

function emptyGroupRow(){
  const row=Object.fromEntries(GROUP_COLS.map(c=>[c,""]));
  row.CONTEST_CODE=contestCodeOf();
  for(const f of (catalog.sections.find(s=>s.id==="TABLE:GROUP")||{fields:[]}).fields){
    if(f.default!=null&&String(f.default)!==""&&!row[f.key]) row[f.key]=String(f.default);
  }
  return tagNewEntityStands(row);
}
/** Подпись варианта поля из каталога; если нет — само значение. */
function variantDisplay(sectionId, key, value){
  const v=String(value||"").trim();
  if(!v) return "";
  const lab=labelForVariant(meta(sectionId,key), v);
  return lab||v;
}
function groupTitle(row,i){
  const parts=[];
  const gc=String(row.GROUP_CODE||"").trim();
  const gv=String(row.GROUP_VALUE||"").trim();
  const method=variantDisplay("TABLE:GROUP","GET_CALC_METHOD",row.GET_CALC_METHOD);
  if(gc) parts.push(gc);
  if(gv) parts.push(gv.length>28?gv.slice(0,28)+"…":gv);
  if(method) parts.push(method);
  if(parts.length) return parts.join(" · ");
  return "Группа "+(i+1);
}
function groupSub(row){
  const parts=[];
  const g=String(row.GET_CALC_CRITERION||"").trim();
  const a1=String(row.ADD_CALC_CRITERION||"").trim();
  const a2=String(row.ADD_CALC_CRITERION_2||"").trim();
  if(g) parts.push(g);
  if(a1) parts.push(a1);
  if(a2) parts.push(a2);
  return parts.join(" · ");
}

/** Скопировать поля строки по списку колонок (полная перезапись dst). */
function copyCols(src, dst, cols, skipKeys){
  const skip=new Set(skipKeys||[]);
  for(const k of cols){
    if(skip.has(k)) continue;
    dst[k]=src[k]!=null?String(src[k]):"";
  }
}
function rewardFlatKeys(){
  return (catalog.sections.find(s=>s.id==="REWARD")||{fields:[]}).fields.map(f=>f.key);
}
function rewardAddLeaves(){
  return (catalog.sections.find(s=>s.id==="REWARD_ADD_DATA")||{fields:[]}).fields.map(f=>jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD"));
}
function applyGroupCopy(fromIdx, toIdxs){
  const rows=data().group||[];
  const src=rows[fromIdx];
  if(!src)return 0;
  let n=0;
  for(const j of toIdxs){
    if(j===fromIdx||j<0||j>=rows.length) continue;
    copyCols(src, rows[j], GROUP_COLS, ["CONTEST_CODE"]);
    rows[j].CONTEST_CODE=contestCodeOf();
    n++;
  }
  pruneLinkGroupCodes(data());
  return n;
}
/** Копировать поля расписания; CONTEST_CODE и TOURNAMENT_CODE у целей не трогаем. */
function applyScheduleCopy(fromIdx, toIdxs){
  const rows=data().schedule||[];
  const src=rows[fromIdx];
  if(!src)return 0;
  let n=0;
  for(const j of toIdxs){
    if(j===fromIdx||j<0||j>=rows.length) continue;
    copyCols(src, rows[j], SCH_COLS, ["CONTEST_CODE","TOURNAMENT_CODE","TARGET_TYPE","FILTER_PERIOD_ARR"]);
    rows[j].CONTEST_CODE=contestCodeOf();
    rows[j].seasonCode=String(src.seasonCode||"");
    rows[j].filter_period=clone(src.filter_period||[]);
    delete rows[j].TARGET_TYPE;
    delete rows[j].FILTER_PERIOD_ARR;
    n++;
  }
  return n;
}
function applyPairCopy(fromIdx, toIdxs){
  const badges=data().badges||[];
  const links=data().reward_link||[];
  const srcLink=links[fromIdx]||{};
  const srcBadge=badges[fromIdx]||{flat:{},add:{}};
  const srcRc=String(srcLink.REWARD_CODE||srcBadge.flat&&srcBadge.flat.REWARD_CODE||"").trim();
  const flatKeys=rewardFlatKeys();
  const addLeaves=rewardAddLeaves();
  let n=0;
  for(const j of toIdxs){
    if(j===fromIdx||j<0||j>=badges.length) continue;
    // REWARD_CODE специально не копируем — иначе дубли кодов наград в CSV
    const keepRc=String((links[j]&&links[j].REWARD_CODE)||"");
    copyCols(srcLink, links[j], LINK_COLS, ["CONTEST_CODE","REWARD_CODE"]);
    links[j].CONTEST_CODE=contestCodeOf();
    links[j].REWARD_CODE=keepRc;
    const flat={};
    for(const k of flatKeys){
      if(k==="REWARD_CODE") continue;
      flat[k]=srcBadge.flat&&srcBadge.flat[k]!=null?String(srcBadge.flat[k]):"";
    }
    flat.REWARD_CODE=keepRc;
    if(!flat.REWARD_TYPE) flat.REWARD_TYPE="BADGE";
    const add={};
    for(const leaf of addLeaves){
      let v=srcBadge.add&&srcBadge.add[leaf]!=null?String(srcBadge.add[leaf]):"";
      // если parent указывал на код источника — перекинуть на код цели
      if(leaf==="parentRewardCode"&&srcRc&&v===srcRc&&keepRc) v=keepRc;
      add[leaf]=v;
    }
    badges[j].flat=flat;
    badges[j].add=add;
    n++;
  }
  return n;
}
function closeCopyModal(){
  document.getElementById("copy-modal-backdrop")?.remove();
}
function openCopyTargetModal(opts){
  closeCopyModal();
  const items=opts.items||[];
  if(!items.length){toast(opts.emptyToast||"Нет куда копировать");return}
  const backdrop=document.createElement("div");
  backdrop.id="copy-modal-backdrop";
  backdrop.className="copy-modal-backdrop";
  backdrop.innerHTML=`
    <div class="copy-modal" role="dialog" aria-modal="true" aria-labelledby="copy-modal-title">
      <h3 id="copy-modal-title">${esc(opts.title||"Куда копировать?")}</h3>
      <p class="copy-modal__text">${esc(opts.text||"")}</p>
      <div class="copy-modal__tools">
        <button type="button" class="btn" id="copy-modal-all">Выбрать все</button>
        <button type="button" class="btn" id="copy-modal-none">Снять все</button>
      </div>
      <div class="copy-modal__list">
        ${items.map(it=>`
          <label class="copy-modal__item">
            <input type="checkbox" data-copy-to="${it.idx}" ${it.checked!==false?"checked":""} />
            <span>${esc(it.label)}</span>
          </label>`).join("")}
      </div>
      <div class="copy-modal__actions">
        <button type="button" class="btn" id="copy-modal-cancel">Отмена</button>
        <button type="button" class="btn btn-primary" id="copy-modal-ok">Копировать</button>
      </div>
    </div>`;
  document.body.appendChild(backdrop);
  const list=backdrop.querySelector(".copy-modal__list");
  const setAll=on=>list.querySelectorAll("input[data-copy-to]").forEach(inp=>{inp.checked=!!on});
  backdrop.querySelector("#copy-modal-all").addEventListener("click",()=>setAll(true));
  backdrop.querySelector("#copy-modal-none").addEventListener("click",()=>setAll(false));
  backdrop.querySelector("#copy-modal-cancel").addEventListener("click",()=>closeCopyModal());
  backdrop.addEventListener("click",e=>{if(e.target===backdrop)closeCopyModal()});
  const onKey=e=>{if(e.key==="Escape"){closeCopyModal();document.removeEventListener("keydown",onKey)}};
  document.addEventListener("keydown",onKey);
  backdrop.querySelector("#copy-modal-ok").addEventListener("click",()=>{
    const selected=[...list.querySelectorAll("input[data-copy-to]:checked")].map(inp=>Number(inp.getAttribute("data-copy-to")));
    if(!selected.length){toast("Выберите хотя бы одну цель");return}
    document.removeEventListener("keydown",onKey);
    closeCopyModal();
    opts.onConfirm(selected);
  });
}
function copyActiveGroupToOthers(){
  const rows=data().group||[];
  if(rows.length<=1){toast("Нет других групп");return}
  const i=activeGroup;
  if(i<0||i>=rows.length)return;
  openCopyTargetModal({
    title:"Копировать группу",
    text:`Источник: «${groupTitle(rows[i],i)}». Отметьте группы, куда перенести все параметры (включая GROUP_CODE).`,
    emptyToast:"Нет других групп",
    items:rows.map((r,idx)=>({idx,label:groupTitle(r,idx),checked:idx!==i})).filter(it=>it.idx!==i),
    onConfirm:(selected)=>{
      const n=applyGroupCopy(i, selected);
      persistLocal();markContestEdited();render();
      toast(n?("Группа скопирована ("+n+")"):"Нечего копировать");
    }
  });
}
function copyActivePairToOthers(){
  const badges=data().badges||[];
  if(badges.length<=1){toast("Нет других связей");return}
  const i=activeLink;
  if(i<0||i>=badges.length)return;
  openCopyTargetModal({
    title:"Копировать связь и награду",
    text:`Источник: «${pairNavTitle(i)}». Копируются все поля связи и награды, кроме кода награды (REWARD_CODE) — у каждой пары он остаётся своим, чтобы не было дублей.`,
    emptyToast:"Нет других связей",
    items:badges.map((_,idx)=>({idx,label:pairNavTitle(idx),checked:idx!==i})).filter(it=>it.idx!==i),
    onConfirm:(selected)=>{
      const n=applyPairCopy(i, selected);
      persistLocal();markContestEdited();render();
      toast(n?("Связь и награда скопированы ("+n+")"):"Нечего копировать");
    }
  });
}
function copyActiveScheduleToOthers(){
  const rows=data().schedule||[];
  if(rows.length<=1){toast("Нет других турниров в расписании");return}
  const i=activeSchedule;
  if(i<0||i>=rows.length)return;
  openCopyTargetModal({
    title:"Копировать расписание",
    text:`Источник: «${scheduleTitle(rows[i],i)}». Отметьте турниры этого конкурса, куда перенести параметры. Код турнира (TOURNAMENT_CODE) у целей не меняется.`,
    emptyToast:"Нет других турниров",
    items:rows.map((r,idx)=>({idx,label:scheduleTitle(r,idx)+(scheduleSub(r)?" · "+scheduleSub(r):""),checked:idx!==i})).filter(it=>it.idx!==i),
    onConfirm:(selected)=>{
      const n=applyScheduleCopy(i, selected);
      ensureScheduleSorted();
      persistLocal();markContestEdited();render();
      toast(n?("Расписание скопировано ("+n+")"):"Нечего копировать");
    }
  });
}

/** Корзина статуса турнира для копирования конкурса. */
function scheduleCopyBucket(status){
  const u=String(status||"").trim().toUpperCase();
  if(u==="АКТИВНЫЙ") return "active";
  if(u==="ПОДВЕДЕНИЕ ИТОГОВ") return "results";
  if(u==="ЗАВЕРШЕН") return "done";
  if(u==="ОТМЕНЕН"||u==="УДАЛЕН") return "dead";
  return "other";
}
function suggestCopiedContestCode(srcCode){
  const base=String(srcCode||"").trim()||"contest";
  const root=base.replace(/_copy\d*$/i,"");
  let cand=root+"_copy";
  let n=2;
  const used=new Set(contests.map(c=>String((c.data&&c.data.contest&&c.data.contest.CONTEST_CODE)||"").trim()).filter(Boolean));
  while(used.has(cand)){cand=root+"_copy"+n;n++}
  return cand;
}
function buildContestDataCopy(src, sel){
  const prevCc=String((src.contest&&src.contest.CONTEST_CODE)||"").trim();
  const nextCc=String(sel.newCode||"").trim();
  const out={
    contest:clone(src.contest||{}),
    feature:clone(src.feature||{}),
    contestPeriod:clone(src.contestPeriod||[]),
    indicator:clone(src.indicator||[]),
    group:[],
    badges:[],
    reward_link:[],
    schedule:[],
  };
  out.contest.CONTEST_CODE=nextCc;
  if(sel.copyNameSuffix!==false){
    const name=String(out.contest.FULL_NAME||"").trim();
    if(name&&!/\(копия\)\s*$/i.test(name)) out.contest.FULL_NAME=name+" (копия)";
  }
  const gIdxs=sel.groupIdxs||[];
  out.group=gIdxs.map(i=>clone((src.group||[])[i])).filter(Boolean);
  if(!out.group.length && !(src.group||[]).length){
    const blank=Object.fromEntries(GROUP_COLS.map(c=>[c,""]));
    for(const f of (catalog.sections.find(s=>s.id==="TABLE:GROUP")||{fields:[]}).fields){
      if(f.kind==="json") continue;
      if(f.default!=null&&String(f.default)!==""&&blank[f.key]!==undefined) blank[f.key]=String(f.default);
    }
    out.group=[blank];
  }
  const pIdxs=sel.pairIdxs||[];
  for(const i of pIdxs){
    const b=(src.badges||[])[i];
    const link=(src.reward_link||[])[i];
    if(!b&&!link) continue;
    out.badges.push(clone(b||emptyBadge()));
    out.reward_link.push(clone(link||Object.fromEntries(LINK_COLS.map(c=>[c,""]))));
  }
  const sIdxs=sel.scheduleIdxs||[];
  out.schedule=sIdxs.map(i=>clone((src.schedule||[])[i])).filter(Boolean);
  if(!out.schedule.length && !(src.schedule||[]).length){
    const blank=Object.fromEntries(SCH_COLS.map(c=>[c,""]));
    for(const f of (catalog.sections.find(s=>s.id==="TABLE:SCHEDULE")||{fields:[]}).fields){
      if(f.key==="TOURNAMENT_CODE"||f.kind==="json") continue;
      if(f.default!=null&&String(f.default)!==""&&blank[f.key]!==undefined) blank[f.key]=String(f.default);
    }
    blank.filter_period=[];
    blank.seasonCode="";
    delete blank.TARGET_TYPE;
    delete blank.FILTER_PERIOD_ARR;
    out.schedule=[blank];
  }
  ensureJsonStructures(out);
  for(const row of out.group||[]) row.CONTEST_CODE=nextCc;
  for(const row of out.indicator||[]) row.CONTEST_CODE=nextCc;
  for(const row of out.reward_link||[]) row.CONTEST_CODE=nextCc;
  for(const row of out.schedule||[]) row.CONTEST_CODE=nextCc;
  resyncPrefixedCodes(out, prevCc);
  syncBadgeSlots(out,false);
  seedLinked(out);
  sortScheduleRows(out.schedule);
  return out;
}
function openCopyContestModal(){
  if(!sessionReady){toast("Сначала создайте или откройте конкурс");return}
  if(activeArchiveId){toast("Копирование из архива недоступно — сначала откройте активный конкурс");return}
  const srcItem=cur();
  if(!srcItem||!srcItem.data){toast("Нет текущего конкурса");return}
  const src=srcItem.data;
  const groups=src.group||[];
  const badges=src.badges||[];
  const schedules=src.schedule||[];
  const srcCode=String((src.contest&&src.contest.CONTEST_CODE)||"").trim();
  const suggested=suggestCopiedContestCode(srcCode);

  const schBuckets={active:[],results:[],done:[],dead:[],other:[]};
  schedules.forEach((r,idx)=>{
    const b=scheduleCopyBucket(r.TOURNAMENT_STATUS);
    (schBuckets[b]||schBuckets.other).push(idx);
  });

  closeCopyModal();
  const backdrop=document.createElement("div");
  backdrop.id="copy-modal-backdrop";
  backdrop.className="copy-modal-backdrop";
  const groupHtml=groups.length?groups.map((r,idx)=>`
    <label class="copy-modal__item">
      <input type="checkbox" data-copy-group="${idx}" checked />
      <span>${esc(groupTitle(r,idx))}${groupSub(r)?" · "+esc(groupSub(r)):""}</span>
    </label>`).join(""):`<p class="copy-modal__text">Групп нет — будет одна пустая.</p>`;
  const pairHtml=badges.length?badges.map((b,idx)=>{
    const flat=(b&&b.flat)||{};
    const code=String(flat.REWARD_CODE||"").trim();
    const name=String(flat.FULL_NAME||"").trim();
    return `<label class="copy-modal__item">
      <input type="checkbox" data-copy-pair="${idx}" checked />
      <span>${esc(pairNavTitle(idx))}${name&&name!==code?" · "+esc(name):""}</span>
    </label>`;
  }).join(""):`<p class="copy-modal__text">Связей/наград нет — будет одна пустая пара.</p>`;

  function schItemClass(bucket){
    if(bucket==="active") return "copy-modal__item--sch-active";
    if(bucket==="results") return "copy-modal__item--sch-results";
    if(bucket==="done") return "copy-modal__item--sch-done";
    if(bucket==="dead") return "copy-modal__item--sch-dead";
    return "";
  }
  const schHtml=schedules.length?schedules.map((r,idx)=>{
    const bucket=scheduleCopyBucket(r.TOURNAMENT_STATUS);
    const lab=scheduleTitle(r,idx)+(scheduleSub(r)?" · "+scheduleSub(r):"");
    return `<label class="copy-modal__item ${schItemClass(bucket)}">
      <input type="checkbox" data-copy-sch="${idx}" data-sch-bucket="${bucket}" checked />
      <span>${esc(lab)}</span>
    </label>`;
  }).join(""):`<p class="copy-modal__text">Турниров нет — будет один пустой период.</p>`;

  const statusBtns=[
    {id:"active",label:"Активные",n:schBuckets.active.length,cls:"copy-modal__status-btn--active"},
    {id:"results",label:"Подведение итогов",n:schBuckets.results.length,cls:"copy-modal__status-btn--results"},
    {id:"done",label:"Завершённые",n:schBuckets.done.length,cls:"copy-modal__status-btn--done"},
    {id:"dead",label:"Отменённые / удалённые",n:schBuckets.dead.length,cls:"copy-modal__status-btn--dead"},
  ].filter(x=>x.n>0);

  backdrop.innerHTML=`
    <div class="copy-modal copy-modal--wide" role="dialog" aria-modal="true" aria-labelledby="copy-modal-title">
      <h3 id="copy-modal-title">Копировать конкурс</h3>
      <p class="copy-modal__text">Будет создан новый конкурс. Карточка, FEATURE, периоды CONTEST_PERIOD и индикаторы копируются целиком. Ниже выберите группы, связи/награды и турниры.</p>
      <div class="copy-modal__field">
        <label for="copy-contest-code">Код нового конкурса (CONTEST_CODE)</label>
        <input type="text" id="copy-contest-code" value="${esc(suggested)}" autocomplete="off" />
      </div>
      <div class="copy-modal__section">
        <div class="copy-modal__section-head">
          <h4 class="copy-modal__section-title">Группы</h4>
          <span class="copy-modal__section-meta">${groups.length} шт.</span>
        </div>
        <div class="copy-modal__tools">
          <button type="button" class="btn" data-sel="group" data-on="1">Все</button>
          <button type="button" class="btn" data-sel="group" data-on="0">Снять</button>
        </div>
        <div class="copy-modal__list" data-list="group">${groupHtml}</div>
      </div>
      <div class="copy-modal__section">
        <div class="copy-modal__section-head">
          <h4 class="copy-modal__section-title">Связи + награды</h4>
          <span class="copy-modal__section-meta">${badges.length} шт.</span>
        </div>
        <div class="copy-modal__tools">
          <button type="button" class="btn" data-sel="pair" data-on="1">Все</button>
          <button type="button" class="btn" data-sel="pair" data-on="0">Снять</button>
        </div>
        <div class="copy-modal__list" data-list="pair">${pairHtml}</div>
      </div>
      <div class="copy-modal__section">
        <div class="copy-modal__section-head">
          <h4 class="copy-modal__section-title">Турниры (расписание)</h4>
          <span class="copy-modal__section-meta">${schedules.length} шт.</span>
        </div>
        ${statusBtns.length?`<div class="copy-modal__status-tools" role="group" aria-label="Выбор по статусу">
          ${statusBtns.map(b=>`<label class="copy-modal__status-btn ${b.cls} is-on" data-status-bucket="${b.id}">
            <input type="checkbox" data-status-toggle="${b.id}" checked />
            <span>${esc(b.label)} (${b.n})</span>
          </label>`).join("")}
        </div>`:""}
        <div class="copy-modal__tools">
          <button type="button" class="btn" data-sel="sch" data-on="1">Все турниры</button>
          <button type="button" class="btn" data-sel="sch" data-on="0">Снять все</button>
        </div>
        <div class="copy-modal__list" data-list="sch">${schHtml}</div>
      </div>
      <div class="copy-modal__actions">
        <button type="button" class="btn" id="copy-modal-cancel">Отмена</button>
        <button type="button" class="btn btn-primary" id="copy-modal-ok">Создать копию</button>
      </div>
    </div>`;
  document.body.appendChild(backdrop);

  const setList=(kind,on)=>{
    backdrop.querySelectorAll(`input[data-copy-${kind}]`).forEach(inp=>{inp.checked=!!on});
    if(kind==="sch"){
      backdrop.querySelectorAll("[data-status-toggle]").forEach(inp=>{
        inp.checked=!!on;
        inp.closest(".copy-modal__status-btn")?.classList.toggle("is-on",!!on);
      });
    }
  };
  backdrop.querySelectorAll("[data-sel]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      setList(btn.getAttribute("data-sel"), btn.getAttribute("data-on")==="1");
    });
  });
  backdrop.querySelectorAll("[data-status-toggle]").forEach(inp=>{
    inp.addEventListener("change",()=>{
      const bucket=inp.getAttribute("data-status-toggle");
      const on=!!inp.checked;
      inp.closest(".copy-modal__status-btn")?.classList.toggle("is-on",on);
      backdrop.querySelectorAll(`input[data-copy-sch][data-sch-bucket="${bucket}"]`).forEach(cb=>{cb.checked=on});
    });
  });
  // Синхронизация статус-кнопок при ручном клике по турниру
  backdrop.querySelectorAll("input[data-copy-sch]").forEach(cb=>{
    cb.addEventListener("change",()=>{
      const bucket=cb.getAttribute("data-sch-bucket");
      if(!bucket) return;
      const boxes=[...backdrop.querySelectorAll(`input[data-copy-sch][data-sch-bucket="${bucket}"]`)];
      const allOn=boxes.length&&boxes.every(x=>x.checked);
      const toggle=backdrop.querySelector(`input[data-status-toggle="${bucket}"]`);
      if(toggle){
        toggle.checked=allOn;
        toggle.closest(".copy-modal__status-btn")?.classList.toggle("is-on",allOn);
      }
    });
  });

  backdrop.querySelector("#copy-modal-cancel").addEventListener("click",()=>closeCopyModal());
  backdrop.addEventListener("click",e=>{if(e.target===backdrop)closeCopyModal()});
  const onKey=e=>{if(e.key==="Escape"){closeCopyModal();document.removeEventListener("keydown",onKey)}};
  document.addEventListener("keydown",onKey);
  backdrop.querySelector("#copy-modal-ok").addEventListener("click",()=>{
    const newCode=String(backdrop.querySelector("#copy-contest-code")?.value||"").trim();
    if(!newCode){toast("Укажите код нового конкурса");return}
    const used=contests.some(c=>String((c.data&&c.data.contest&&c.data.contest.CONTEST_CODE)||"").trim()===newCode);
    if(used&&!confirm(`Код «${newCode}» уже есть в списке. Всё равно создать копию?`)) return;
    const groupIdxs=[...backdrop.querySelectorAll("input[data-copy-group]:checked")].map(el=>Number(el.getAttribute("data-copy-group")));
    const pairIdxs=[...backdrop.querySelectorAll("input[data-copy-pair]:checked")].map(el=>Number(el.getAttribute("data-copy-pair")));
    const scheduleIdxs=[...backdrop.querySelectorAll("input[data-copy-sch]:checked")].map(el=>Number(el.getAttribute("data-copy-sch")));
    document.removeEventListener("keydown",onKey);
    closeCopyModal();
    const dataCopy=buildContestDataCopy(src,{newCode,groupIdxs,pairIdxs,scheduleIdxs});
    const item={id:"c"+Date.now(),name:String(dataCopy.contest.FULL_NAME||"").trim(),stands:contestItemStands(src),data:dataCopy,baseline:null,userEdited:false};
    migrateContestStands(item, {block:projectBlock});
    markBaseline(item);
    contests.push(item);
    activeContest=contests.length-1;
    activeArchiveId=null;
    activeSection="CONTEST";
    activeBadge=0;activeSchedule=0;activeIndicator=0;activeGroup=0;activeLink=0;
    activePairFocus=null;
    persistLocal();
    render();
    toast(`Копия создана · ${newCode} · G:${groupIdxs.length} R:${pairIdxs.length} T:${scheduleIdxs.length}`);
  });
}

/* ——— Архив удалений (бандл на конкурс) ——— */
var ARCHIVE_KIND_LABEL={
  bundle:"Архив конкурса",
  contest:"Конкурс целиком",
  group:"Группа",
  schedule:"Расписание",
  indicator:"Индикатор",
  pair:"Связь + награда",
};
function newArchiveId(){return "arch_"+Date.now().toString(36)+"_"+Math.random().toString(36).slice(2,7)}
function newFragmentId(){return "frag_"+Date.now().toString(36)+"_"+Math.random().toString(36).slice(2,7)}
function archiveKindLabel(kind){return ARCHIVE_KIND_LABEL[kind]||String(kind||"элемент")}
function findArchiveEntry(id){return archiveEntries.find(e=>e&&e.id===id)||null}
function removeArchiveEntry(id){
  const i=archiveEntries.findIndex(e=>e&&e.id===id);
  if(i<0) return null;
  const [e]=archiveEntries.splice(i,1);
  return e;
}
function archiveBundleKey(contestId, contestCode){
  const id=String(contestId||"").trim();
  if(id) return "id:"+id;
  const code=String(contestCode||"").trim();
  if(code) return "code:"+code;
  return "";
}
function findArchiveBundleFor(contestId, contestCode){
  const want=archiveBundleKey(contestId, contestCode);
  if(!want) return null;
  return archiveEntries.find(e=>{
    if(!e||e.kind!=="bundle") return false;
    return archiveBundleKey(e.contestId, e.contestCode)===want;
  })||null;
}
function findLiveContestForArchive(entry){
  if(!entry) return null;
  if(entry.contestId){
    const byId=contests.find(c=>c&&c.id===entry.contestId);
    if(byId) return byId;
  }
  const code=String(entry.contestCode||"").trim();
  if(code){
    const byCode=contests.find(c=>String((c.data.contest&&c.data.contest.CONTEST_CODE)||"").trim()===code);
    if(byCode) return byCode;
  }
  return null;
}
function emptyArchiveBundle(meta){
  return{
    id:newArchiveId(),
    kind:"bundle",
    contestId:String((meta&&meta.contestId)||""),
    contestCode:String((meta&&meta.contestCode)||"").trim(),
    contestName:String((meta&&meta.contestName)||"").trim(),
    label:String((meta&&meta.label)||(meta&&meta.contestCode)||(meta&&meta.contestName)||"Конкурс"),
    deletedAt:new Date().toISOString(),
    whole:false,
    snapshot:null,
    fragments:[],
  };
}
function touchArchiveBundle(b){
  if(!b) return;
  b.deletedAt=new Date().toISOString();
  const code=String(b.contestCode||"").trim();
  const name=String(b.contestName||"").trim();
  b.label=code||name||b.label||"Конкурс";
}
function ensureArchiveBundle(meta){
  let b=findArchiveBundleFor(meta.contestId, meta.contestCode);
  if(b){
    if(meta.contestId&&!b.contestId) b.contestId=meta.contestId;
    if(meta.contestCode&&!b.contestCode) b.contestCode=meta.contestCode;
    if(meta.contestName) b.contestName=meta.contestName;
    touchArchiveBundle(b);
    // поднять в начало списка
    const i=archiveEntries.indexOf(b);
    if(i>0){archiveEntries.splice(i,1);archiveEntries.unshift(b)}
    return b;
  }
  b=emptyArchiveBundle(meta);
  archiveEntries.unshift(b);
  contestListShowArchive=true;
  syncContestKindFilterButtons();
  return b;
}
function pushArchiveFragment(bundle, kind, payload, label, detail){
  if(!bundle.fragments) bundle.fragments=[];
  const frag={
    id:newFragmentId(),
    kind,
    deletedAt:new Date().toISOString(),
    label:label||archiveKindLabel(kind),
    detail:detail||"",
    payload:clone(payload),
  };
  bundle.fragments.unshift(frag);
  touchArchiveBundle(bundle);
  return frag;
}
function mergeFragmentsIntoSnapshotData(data, fragments){
  const d=data||emptyContestData();
  if(!Array.isArray(d.group)) d.group=[];
  if(!Array.isArray(d.indicator)) d.indicator=[];
  if(!Array.isArray(d.schedule)) d.schedule=[];
  if(!Array.isArray(d.badges)) d.badges=[];
  if(!Array.isArray(d.reward_link)) d.reward_link=[];
  for(const f of (fragments||[])){
    if(!f) continue;
    if(f.kind==="group") d.group.push(clone(f.payload||{}));
    else if(f.kind==="indicator") d.indicator.push(clone(f.payload||{}));
    else if(f.kind==="schedule") d.schedule.push(clone(f.payload||{}));
    else if(f.kind==="pair"){
      const p=f.payload||{};
      d.badges.push(clone(p.badge||emptyBadge()));
      d.reward_link.push(clone(p.link||emptyLinkRow()));
    }
  }
  return d;
}
function archiveContestAt(i){
  if(i<0||i>=contests.length) return;
  if(contests.length<=1){toast("Нельзя отправить в архив единственный конкурс");return}
  const c=contests[i];
  const code=String((c.data.contest&&c.data.contest.CONTEST_CODE)||"").trim();
  const name=String((c.data.contest&&c.data.contest.FULL_NAME)||"").trim();
  const bundle=ensureArchiveBundle({
    contestId:c.id,
    contestCode:code,
    contestName:name,
    label:code||name||("Конкурс "+(i+1)),
  });
  // сливаем уже лежащие фрагменты в полный снимок
  const data=mergeFragmentsIntoSnapshotData(clone(c.data), bundle.fragments);
  expandContestPrefixedCodes(data);
  syncBadgeSlots(data,false);
  seedLinked(data);
  sortScheduleRows(data.schedule);
  bundle.whole=true;
  bundle.snapshot={id:c.id,name:c.name||"",data};
  bundle.fragments=[];
  touchArchiveBundle(bundle);
  if(c&&c.id) selectedContestIds.delete(c.id);
  const wasActive=activeContest===i;
  contests.splice(i,1);
  if(activeContest>=contests.length) activeContest=contests.length-1;
  else if(activeContest>i) activeContest--;
  if(wasActive){activeArchiveId=bundle.id;activeArchiveSection=defaultArchiveSection(bundle);activeSection="ARCHIVE"}
  persistLocal();
  render();
  toast("Конкурс перемещён в архив");
}
function archiveFragment(kind, payload, label, detail){
  const c=cur();
  if(!c) return;
  const code=contestCodeOf();
  const name=String((c.data.contest&&c.data.contest.FULL_NAME)||"").trim();
  const bundle=ensureArchiveBundle({
    contestId:c.id,
    contestCode:code,
    contestName:name,
    label:code||name||"Конкурс",
  });
  // если уже «целиком» — фрагменты не добавляем в отдельный список: дополняем снимок
  if(bundle.whole&&bundle.snapshot&&bundle.snapshot.data){
    const d=bundle.snapshot.data;
    if(kind==="group"){if(!d.group)d.group=[];d.group.push(clone(payload))}
    else if(kind==="indicator"){if(!d.indicator)d.indicator=[];d.indicator.push(clone(payload))}
    else if(kind==="schedule"){if(!d.schedule)d.schedule=[];d.schedule.push(clone(payload))}
    else if(kind==="pair"){
      if(!d.badges)d.badges=[];if(!d.reward_link)d.reward_link=[];
      const p=payload||{};
      d.badges.push(clone(p.badge||emptyBadge()));
      d.reward_link.push(clone(p.link||emptyLinkRow()));
    }
    touchArchiveBundle(bundle);
  }else{
    pushArchiveFragment(bundle, kind, payload, label, detail);
  }
  contestListShowArchive=true;
  syncContestKindFilterButtons();
}
/** Удаление в архив: группа / индикатор / расписание / пара. confirmAsk=false — без диалога. */
function deleteGroupAt(di, opts){
  opts=opts||{};
  const rows=data().group||[];
  if(di<0||di>=rows.length) return false;
  if(opts.confirmAsk!==false && !confirm("Переместить эту группу в архив?")) return false;
  const row=rows[di];
  archiveFragment("group",row,groupTitle(row,di),contestCodeOf());
  rows.splice(di,1);
  if(activeGroup>=rows.length) activeGroup=Math.max(0,rows.length-1);
  activeSection=rows.length?"GROUP:"+(activeGroup+1):"GROUP";
  pruneLinkGroupCodes(data());
  persistLocal();markContestEdited();
  if(opts.toast!==false) toast("Группа перемещена в архив");
  return true;
}
function deleteIndicatorAt(di, opts){
  opts=opts||{};
  const rows=data().indicator||[];
  if(di<0||di>=rows.length) return false;
  if(opts.confirmAsk!==false && !confirm("Переместить этот индикатор в архив?")) return false;
  const row=rows[di];
  archiveFragment("indicator",row,indicatorTitle(row,di),contestCodeOf());
  rows.splice(di,1);
  if(activeIndicator>=rows.length) activeIndicator=Math.max(0,rows.length-1);
  activeSection=rows.length?"INDICATOR:"+(activeIndicator+1):"INDICATOR";
  persistLocal();markContestEdited();
  if(opts.toast!==false) toast("Индикатор перемещён в архив");
  return true;
}
function deleteScheduleAt(di, opts){
  opts=opts||{};
  const rows=data().schedule||[];
  if(di<0||di>=rows.length) return false;
  if(opts.confirmAsk!==false && !confirm("Переместить этот турнир расписания в архив?")) return false;
  const row=rows[di];
  archiveFragment("schedule",row,scheduleTitle(row,di),scheduleSub(row)||contestCodeOf());
  rows.splice(di,1);
  if(activeSchedule>=rows.length) activeSchedule=Math.max(0,rows.length-1);
  activeSection=rows.length?"SCHEDULE:"+(activeSchedule+1):"SCHEDULE";
  persistLocal();markContestEdited();
  if(opts.toast!==false) toast("Турнир перемещён в архив");
  return true;
}
function deletePairAt(di, opts){
  opts=opts||{};
  const badges=data().badges||[];
  const links=data().reward_link||[];
  if(di<0||di>=badges.length) return false;
  if(opts.confirmAsk!==false && !confirm("Переместить эту пару (связь + награда) в архив?")) return false;
  const badge=badges[di];
  const link=links[di];
  archiveFragment("pair",{badge,link},pairNavTitle(di),contestCodeOf());
  const itemsBefore=contestItemPairIndexes(cur());
  let nextFocus=activePairFocus;
  if(activePairFocus===di){
    const after=itemsBefore.filter(x=>x>di);
    const before=itemsBefore.filter(x=>x<di);
    const pick=after.length?after[0]:(before.length?before[before.length-1]:null);
    nextFocus=pick==null?null:(pick>di?pick-1:pick);
  }else if(activePairFocus!=null && activePairFocus>di){
    nextFocus=activePairFocus-1;
  }
  badges.splice(di,1);
  links.splice(di,1);
  if(activeLink>=badges.length) activeLink=Math.max(0,badges.length-1);
  activePairFocus=nextFocus;
  if(nextFocus!=null){
    activeLink=nextFocus;
    activeBadge=nextFocus;
    activeSection="PAIR:"+(nextFocus+1);
  }else{
    activeBadge=activeLink;
    activeSection=badges.length?"PAIR:"+(activeLink+1):"PAIR";
  }
  persistLocal();markContestEdited();
  if(opts.toast!==false) toast("Пара перемещена в архив");
  return true;
}
function handleNavDelete(kind, idx){
  let ok=false;
  if(kind==="group") ok=deleteGroupAt(idx);
  else if(kind==="indicator") ok=deleteIndicatorAt(idx);
  else if(kind==="schedule") ok=deleteScheduleAt(idx);
  else if(kind==="pair") ok=deletePairAt(idx);
  if(ok) render();
}

function archiveBundleStats(b){
  if(!b) return {r:0,t:0,g:0,i:0,parts:0,whole:false};
  if(b.whole&&b.snapshot&&b.snapshot.data){
    const d=b.snapshot.data;
    return{
      r:(d.badges||[]).length,
      t:(d.schedule||[]).length,
      g:(d.group||[]).length,
      i:(d.indicator||[]).length,
      parts:0,
      whole:true,
    };
  }
  const fr=b.fragments||[];
  let r=0,t=0,g=0,i=0;
  for(const f of fr){
    if(f.kind==="pair") r++;
    else if(f.kind==="schedule") t++;
    else if(f.kind==="group") g++;
    else if(f.kind==="indicator") i++;
  }
  return{r,t,g,i,parts:fr.length,whole:false};
}
function archiveBundleListHtml(b){
  const code=esc(String(b.contestCode||b.label||"Конкурс").trim()||"Конкурс");
  const st=archiveBundleStats(b);
  const mark=st.whole?'<span class="ct-kind">целиком</span>':'<span class="ct-kind">части</span>';
  return mark+`<span class="ct-code">${code}`+
    `<span class="ct-dash" aria-hidden="true">-</span><span class="ct-stat">R:&nbsp;${st.r}</span>`+
    `<span class="ct-sep" aria-hidden="true">·</span><span class="ct-stat">T:&nbsp;${st.t}</span>`+
    `<span class="ct-sep" aria-hidden="true">·</span><span class="ct-stat">G:&nbsp;${st.g}</span></span>`;
}
function archiveEntrySearchTokens(e){
  const parts=[e.label,e.contestCode,e.contestName,e.deletedAt,e.kind];
  if(e.whole) parts.push("целиком","contest");
  for(const f of (e.fragments||[])){
    parts.push(f.kind,f.label,f.detail,archiveKindLabel(f.kind));
  }
  if(e.snapshot&&e.snapshot.data){
    const d=e.snapshot.data;
    parts.push((d.contest||{}).CONTEST_CODE,(d.contest||{}).FULL_NAME);
  }
  return searchFieldTokens(parts);
}
function archiveMatchesListQuery(e,q){
  return tokensMatchQuery(archiveEntrySearchTokens(e), q);
}
function formatArchiveWhen(iso){
  const s=String(iso||"");
  if(s.length>=16) return s.slice(0,10)+" "+s.slice(11,16);
  return s||"—";
}

function liveRewardCodes(d){
  const set=new Set();
  for(const row of (d.reward_link||[])){
    const c=String(row.REWARD_CODE||"").trim();
    if(c) set.add(c);
  }
  for(const b of (d.badges||[])){
    const c=String((b.flat&&b.flat.REWARD_CODE)||"").trim();
    if(c) set.add(c);
  }
  return set;
}
function liveRewardCodesExcept(d, skipIdx){
  const set=new Set();
  (d.reward_link||[]).forEach((row,i)=>{
    if(i===skipIdx) return;
    const c=String(row.REWARD_CODE||"").trim();
    if(c) set.add(c);
  });
  (d.badges||[]).forEach((b,i)=>{
    if(i===skipIdx) return;
    const c=String((b.flat&&b.flat.REWARD_CODE)||"").trim();
    if(c) set.add(c);
  });
  return set;
}
function liveTournamentCodes(d){
  const set=new Set();
  for(const row of (d.schedule||[])){
    const c=String(row.TOURNAMENT_CODE||"").trim();
    if(c) set.add(c);
  }
  return set;
}
function uniquifyPrefixedCode(fullCode, existing, kind, cc){
  const code=String(fullCode||"").trim();
  if(code&&!existing.has(code)) return code;
  if(kind==="item" || followsItemPrefix(code)){
    if(code && !followsItemPrefix(code)){
      for(let n=2;n<200;n++){
        const next=code+"_"+n;
        if(!existing.has(next)) return next;
      }
      return code+"_r"+Date.now().toString(36);
    }
    let suf=endingFromItemCode(code);
    if(suf==null) suf="";
    for(let n=2;n<200;n++){
      const next=buildItemCode(suf?(suf+"_"+n):String(n));
      if(!existing.has(next)) return next;
    }
    return buildItemCode("r"+Date.now().toString(36));
  }
  if(code && !followsPrefixedPrinciple(code, kind, cc)){
    for(let n=2;n<200;n++){
      const next=code+"_"+n;
      if(!existing.has(next)) return next;
    }
    return code+"_r"+Date.now().toString(36);
  }
  const prefix=kind==="reward"?rewardCodePrefix(cc):tournamentCodePrefix(cc);
  let suf=endingFromFullCode(code, cc, kind);
  if(suf==null) suf="";
  for(let n=2;n<200;n++){
    const next=buildPrefixedCode(prefix, suf?(suf+"_"+n):String(n));
    if(!existing.has(next)) return next;
  }
  return buildPrefixedCode(prefix, "r"+Date.now().toString(36));
}
/** choice: keep | replace | alongside — синхронный выбор через prompt. */
function askCodeConflict(what, code){
  const ans=prompt(
    what+" «"+code+"» уже есть в конкурсе.\n\n"+
    "1 — оставить текущую (не восстанавливать эту запись)\n"+
    "2 — заменить текущую архивной\n"+
    "3 — восстановить рядом (новый код)\n\n"+
    "Введите 1, 2 или 3:",
    "3"
  );
  const v=String(ans||"").trim();
  if(v==="1") return "keep";
  if(v==="2") return "replace";
  if(v==="3") return "alongside";
  return null; // отмена
}
function applyPairToContest(d, pairPayload, mode){
  // mode: add | replace@index
  const p=pairPayload||{};
  const badge=clone(p.badge||emptyBadge());
  const link=clone(p.link||emptyLinkRow());
  const cc=contestCodeOf(d);
  link.CONTEST_CODE=cc;
  let rc=String((badge.flat&&badge.flat.REWARD_CODE)||link.REWARD_CODE||"").trim();
  const existing=liveRewardCodes(d);
  if(mode&&String(mode).startsWith("replace@")){
    const idx=Number(String(mode).split("@")[1]);
    if(idx>=0&&idx<(d.badges||[]).length){
      d.badges[idx]=badge;
      d.reward_link[idx]=link;
      badge.flat.REWARD_CODE=rc;
      link.REWARD_CODE=rc;
      activeLink=idx;activeBadge=idx;
      activeSection="PAIR:"+(idx+1);
      return true;
    }
  }
  if(rc&&existing.has(rc)){
    const choice=askCodeConflict("Код награды", rc);
    if(choice===null) return false;
    if(choice==="keep") return "skip";
    if(choice==="replace"){
      const idx=(d.reward_link||[]).findIndex(r=>String(r.REWARD_CODE||"").trim()===rc);
      const j=idx>=0?idx:(d.badges||[]).findIndex(b=>String((b.flat&&b.flat.REWARD_CODE)||"").trim()===rc);
      if(j>=0){
        d.badges[j]=badge;
        d.reward_link[j]=link;
        badge.flat.REWARD_CODE=rc;
        link.REWARD_CODE=rc;
        activeLink=j;activeBadge=j;
        activeSection="PAIR:"+(j+1);
        return true;
      }
    }
    // alongside
    rc=uniquifyPrefixedCode(rc, existing, "reward", cc);
    link.REWARD_CODE=rc;
    if(badge.flat) badge.flat.REWARD_CODE=rc;
  }
  if(!Array.isArray(d.badges)) d.badges=[];
  if(!Array.isArray(d.reward_link)) d.reward_link=[];
  d.badges.push(badge);
  d.reward_link.push(link);
  const j=d.badges.length-1;
  activeLink=j;activeBadge=j;
  activeSection="PAIR:"+(j+1);
  return true;
}
function applyScheduleToContest(d, rowIn){
  const row=clone(rowIn||{});
  const cc=contestCodeOf(d);
  row.CONTEST_CODE=cc;
  let tc=String(row.TOURNAMENT_CODE||"").trim();
  const existing=liveTournamentCodes(d);
  if(tc&&existing.has(tc)){
    const choice=askCodeConflict("Код турнира", tc);
    if(choice===null) return false;
    if(choice==="keep") return "skip";
    if(choice==="replace"){
      const j=d.schedule.findIndex(r=>String(r.TOURNAMENT_CODE||"").trim()===tc);
      if(j>=0){
        d.schedule[j]=row;
        sortScheduleRows(d.schedule);
        const ni=d.schedule.findIndex(r=>String(r.TOURNAMENT_CODE||"").trim()===tc);
        activeSchedule=Math.max(0,ni);
        activeSection="SCHEDULE:"+(activeSchedule+1);
        return true;
      }
    }
    tc=uniquifyPrefixedCode(tc, existing, "tournament", cc);
    row.TOURNAMENT_CODE=tc;
  }
  if(!Array.isArray(d.schedule)) d.schedule=[];
  d.schedule.push(row);
  sortScheduleRows(d.schedule);
  const ni=tc?d.schedule.findIndex(r=>String(r.TOURNAMENT_CODE||"").trim()===tc):d.schedule.length-1;
  activeSchedule=Math.max(0,ni>=0?ni:d.schedule.length-1);
  activeSection="SCHEDULE:"+(activeSchedule+1);
  return true;
}
function applyGroupToContest(d, row){
  if(!Array.isArray(d.group)) d.group=[];
  const r=clone(row||{});
  r.CONTEST_CODE=contestCodeOf(d);
  d.group.push(r);
  pruneLinkGroupCodes(d);
  activeGroup=d.group.length-1;
  activeSection="GROUP:"+(activeGroup+1);
  return true;
}
function applyIndicatorToContest(d, row){
  if(!Array.isArray(d.indicator)) d.indicator=[];
  const r=clone(row||{});
  r.CONTEST_CODE=contestCodeOf(d);
  d.indicator.push(r);
  activeIndicator=d.indicator.length-1;
  activeSection="INDICATOR:"+(activeIndicator+1);
  return true;
}
function restoreFragmentIntoLive(live, frag){
  if(!live||!frag) return false;
  const d=live.data;
  let r=true;
  if(frag.kind==="group") r=applyGroupToContest(d, frag.payload);
  else if(frag.kind==="indicator") r=applyIndicatorToContest(d, frag.payload);
  else if(frag.kind==="schedule") r=applyScheduleToContest(d, frag.payload);
  else if(frag.kind==="pair"){
    const rec=recommendedBadges(d.contest.CONTEST_TYPE);
    const curN=(d.badges||[]).length;
    if(curN>=rec){
      if(!confirm("Для типа конкурса обычно до "+rec+" пар(ы). Сейчас уже "+curN+". Всё равно восстановить ещё одну?")) return false;
    }
    r=applyPairToContest(d, frag.payload);
  }else return false;
  if(r==="skip") return "skip";
  return !!r;
}
function removeFragmentFromBundle(bundle, fragId){
  if(!bundle||!bundle.fragments) return;
  bundle.fragments=bundle.fragments.filter(f=>f&&f.id!==fragId);
  touchArchiveBundle(bundle);
  if(!bundle.whole&&!(bundle.fragments||[]).length){
    removeArchiveEntry(bundle.id);
  }
}
function restoreContestWholeFromBundle(bundle, parts){
  const snap=bundle.snapshot;
  if(!snap||!snap.data){toast("В архиве нет полного снимка конкурса");return false}
  if(findLiveContestForArchive(bundle)){
    toast("Конкурс уже в списке — восстановите отдельные части");
    return false;
  }
  const src=snap.data;
  const empty=emptyContestData();
  const data=clone(src);
  if(parts&&!parts.all){
    if(!parts.card){data.contest=clone(empty.contest);data.feature=clone(empty.feature);
      // сохранить код/имя для идентификации
      if(src.contest){
        data.contest.CONTEST_CODE=src.contest.CONTEST_CODE||"";
        data.contest.FULL_NAME=src.contest.FULL_NAME||"";
        data.contest.CONTEST_TYPE=src.contest.CONTEST_TYPE||data.contest.CONTEST_TYPE;
      }
    }
    if(!parts.groups) data.group=clone(empty.group);
    if(!parts.indicators) data.indicator=clone(empty.indicator);
    if(!parts.schedule) data.schedule=clone(empty.schedule);
    if(!parts.pairs){data.badges=clone(empty.badges);data.reward_link=clone(empty.reward_link)}
  }
  expandContestPrefixedCodes(data);
  syncBadgeSlots(data,false);
  seedLinked(data);
  sortScheduleRows(data.schedule);
  const item={id:snap.id||("c"+Date.now()),name:snap.name||"",data,baseline:null,userEdited:false};
  markBaseline(item);
  contests.push(item);
  activeContest=contests.length-1;
  activeArchiveId=null;
  activeSection="CONTEST";
  activeBadge=0;activeSchedule=0;activeIndicator=0;activeGroup=0;activeLink=0;
  activePairFocus=null;
  if(parts&&!parts.all){
    bundle.whole=false;
    bundle.snapshot=null;
    bundle.fragments=[];
    if(!parts.groups) for(const row of (src.group||[])) pushArchiveFragment(bundle,"group",row,groupTitle(row,0),"");
    if(!parts.indicators) for(const row of (src.indicator||[])) pushArchiveFragment(bundle,"indicator",row,indicatorTitle(row,0),"");
    if(!parts.schedule) for(const row of (src.schedule||[])) pushArchiveFragment(bundle,"schedule",row,scheduleTitle(row,0),"");
    if(!parts.pairs){
      const badges=src.badges||[];
      const links=src.reward_link||[];
      for(let i=0;i<badges.length;i++) pushArchiveFragment(bundle,"pair",{badge:badges[i],link:links[i]||{}},"Пара "+(i+1),"");
    }
    if(!(bundle.fragments||[]).length) removeArchiveEntry(bundle.id);
    else{
      touchArchiveBundle(bundle);
      activeArchiveId=bundle.id;
    }
  }else{
    removeArchiveEntry(bundle.id);
  }
  return true;
}

function restoreAllFragmentsFromBundle(bundle){
  const live=findLiveContestForArchive(bundle);
  if(!live){toast("Живой конкурс не найден — сначала восстановите конкурс целиком или создайте его");return false}
  const fr=[...(bundle.fragments||[])];
  let done=0, skipped=0;
  for(const frag of fr){
    const r=restoreFragmentIntoLive(live, frag);
    if(r===false) return false;
    if(r==="skip"){skipped++;continue}
    removeFragmentFromBundle(bundle, frag.id);
    done++;
  }
  activeContest=contests.indexOf(live);
  activeArchiveId=findArchiveEntry(bundle.id)?bundle.id:null;
  live.userEdited=true;
  seedLinked(live.data);
  if(done||skipped) toast("Восстановлено: "+done+(skipped?", пропущено: "+skipped:""));
  return done>0||skipped>0;
}
function openArchiveRestoreModal(entry){
  if(!entry||entry.kind!=="bundle") return;
  closeCopyModal();
  const backdrop=document.createElement("div");
  backdrop.id="copy-modal-backdrop";
  backdrop.className="copy-modal-backdrop";
  const live=findLiveContestForArchive(entry);
  const st=archiveBundleStats(entry);
  const fr=entry.fragments||[];
  let body="";
  if(entry.whole&&entry.snapshot){
    body=`
      <p class="copy-modal__text">Восстановить архив конкурса «${esc(entry.label)}» в список?</p>
      <label class="copy-modal__item"><input type="radio" name="arch-mode" value="all" checked /><span>Всё, что в архиве (конкурс целиком)</span></label>
      <label class="copy-modal__item"><input type="radio" name="arch-mode" value="parts" /><span>Только выбранные разделы</span></label>
      <div class="copy-modal__list" id="arch-parts" hidden>
        <label class="copy-modal__item"><input type="checkbox" data-part="card" checked /><span>Карточка + FEATURE</span></label>
        <label class="copy-modal__item"><input type="checkbox" data-part="groups" checked /><span>Группы (${st.g})</span></label>
        <label class="copy-modal__item"><input type="checkbox" data-part="indicators" checked /><span>Индикаторы (${st.i})</span></label>
        <label class="copy-modal__item"><input type="checkbox" data-part="schedule" checked /><span>Расписание (${st.t})</span></label>
        <label class="copy-modal__item"><input type="checkbox" data-part="pairs" checked /><span>Связи + награды (${st.r})</span></label>
      </div>`;
  }else{
    const where=live
      ?(`Вернуть архивные части в конкурс «${esc(contestCodeLine(live,contests.indexOf(live)))}»?`)
      :(`Конкурс «${esc(entry.label)}» сейчас не в списке — части восстановить нельзя, пока нет живого конкурса.`);
    const byKind={group:[],indicator:[],schedule:[],pair:[]};
    for(const f of fr){if(byKind[f.kind]) byKind[f.kind].push(f)}
    body=`<p class="copy-modal__text">${where}</p>
      <label class="copy-modal__item"><input type="radio" name="arch-mode" value="all" checked /><span>Все архивные части (${fr.length})</span></label>
      <label class="copy-modal__item"><input type="radio" name="arch-mode" value="kinds" /><span>Только выбранные типы</span></label>
      <div class="copy-modal__list" id="arch-parts" hidden>
        ${byKind.group.length?`<label class="copy-modal__item"><input type="checkbox" data-kind="group" checked /><span>Группы (${byKind.group.length})</span></label>`:""}
        ${byKind.indicator.length?`<label class="copy-modal__item"><input type="checkbox" data-kind="indicator" checked /><span>Индикаторы (${byKind.indicator.length})</span></label>`:""}
        ${byKind.schedule.length?`<label class="copy-modal__item"><input type="checkbox" data-kind="schedule" checked /><span>Расписание (${byKind.schedule.length})</span></label>`:""}
        ${byKind.pair.length?`<label class="copy-modal__item"><input type="checkbox" data-kind="pair" checked /><span>Связи + награды (${byKind.pair.length})</span></label>`:""}
      </div>
      <p class="copy-modal__text" style="margin-top:8px;font-size:12px;opacity:.85">При совпадении кода награды/турнира спросим: оставить, заменить или рядом.</p>`;
  }
  backdrop.innerHTML=`
    <div class="copy-modal" role="dialog" aria-modal="true" aria-labelledby="copy-modal-title">
      <h3 id="copy-modal-title">Восстановить из архива</h3>
      ${body}
      <div class="copy-modal__actions">
        <button type="button" class="btn" id="copy-modal-cancel">Отмена</button>
        <button type="button" class="btn btn-primary" id="copy-modal-ok" ${(!entry.whole&&!live)?"disabled":""}>Восстановить</button>
      </div>
    </div>`;
  document.body.appendChild(backdrop);
  const partsBox=backdrop.querySelector("#arch-parts");
  backdrop.querySelectorAll('input[name="arch-mode"]').forEach(r=>{
    r.addEventListener("change",()=>{
      const v=backdrop.querySelector('input[name="arch-mode"]:checked')?.value;
      if(partsBox) partsBox.hidden=!(v==="parts"||v==="kinds");
    });
  });
  backdrop.querySelector("#copy-modal-cancel").addEventListener("click",()=>closeCopyModal());
  backdrop.addEventListener("click",e=>{if(e.target===backdrop)closeCopyModal()});
  const onKey=e=>{if(e.key==="Escape"){closeCopyModal();document.removeEventListener("keydown",onKey)}};
  document.addEventListener("keydown",onKey);
  backdrop.querySelector("#copy-modal-ok").addEventListener("click",()=>{
    const mode=backdrop.querySelector('input[name="arch-mode"]:checked')?.value||"all";
    let ok=false;
    if(entry.whole){
      if(mode==="all") ok=restoreContestWholeFromBundle(entry,{all:true});
      else{
        const parts={all:false};
        backdrop.querySelectorAll("[data-part]").forEach(inp=>{parts[inp.getAttribute("data-part")]=!!inp.checked});
        if(!parts.card&&!parts.groups&&!parts.indicators&&!parts.schedule&&!parts.pairs){
          toast("Выберите хотя бы один раздел");return;
        }
        ok=restoreContestWholeFromBundle(entry,parts);
      }
    }else{
      if(!live){toast("Нет конкурса для восстановления частей");return}
      if(mode==="all") ok=restoreAllFragmentsFromBundle(entry);
      else{
        const kinds=new Set();
        backdrop.querySelectorAll("[data-kind]:checked").forEach(inp=>kinds.add(inp.getAttribute("data-kind")));
        if(!kinds.size){toast("Выберите хотя бы один тип");return}
        const frags=[...(entry.fragments||[])].filter(f=>kinds.has(f.kind));
        let done=0,skipped=0;
        for(const frag of frags){
          const r=restoreFragmentIntoLive(live, frag);
          if(r===false){document.removeEventListener("keydown",onKey);closeCopyModal();persistLocal();render();return}
          if(r==="skip"){skipped++;continue}
          removeFragmentFromBundle(entry, frag.id);
          done++;
        }
        activeContest=contests.indexOf(live);
        live.userEdited=true;
        seedLinked(live.data);
        ok=done>0||skipped>0;
        if(ok) toast("Восстановлено: "+done+(skipped?", пропущено: "+skipped:""));
      }
    }
    document.removeEventListener("keydown",onKey);
    closeCopyModal();
    if(ok){persistLocal();render();if(entry.whole) toast("Восстановлено из архива")}
  });
}
function purgeArchiveEntry(entry){
  if(!entry) return;
  if(!confirm("Безвозвратно удалить архив конкурса «"+entry.label+"» со всеми частями? Это нельзя отменить.")) return;
  if(activeArchiveId===entry.id) activeArchiveId=null;
  removeArchiveEntry(entry.id);
  persistLocal();
  render();
  toast("Удалено из архива безвозвратно");
}
function lockLayout(layout){
  return (layout||[]).map(g=>({
    title:g.title,
    hint:g.hint,
    items:(g.items||[]).map(it=>Object.assign({},it,{
      locked:true,
      lockedHint:"Архив — только просмотр",
      pickFromGroups:false,
      pickVariants:null,
      compositeKind:"",
    })),
  }));
}
function appendLockedGrouped(host, sectionId, layout, getRaw, pathPrefix, omitKeys){
  if(!host) return;
  host.appendChild(renderGrouped(
    sectionId,
    lockLayout(layout),
    f=>String(getRaw(f)??""),
    ()=>{},
    f=>(pathPrefix||"archive.")+((f&&f.key)||"x"),
    omitKeys||[],
    {locked:true,lockedHint:"Архив — только просмотр"}
  ));
}
function defaultArchiveSection(entry){
  if(!entry) return "CONTEST";
  if(entry.whole&&entry.snapshot) return "CONTEST";
  const fr=entry.fragments||[];
  if(!fr.length) return "CONTEST";
  const f=fr[0];
  if(f.kind==="group") return "GROUP:"+f.id;
  if(f.kind==="indicator") return "INDICATOR:"+f.id;
  if(f.kind==="schedule") return "SCHEDULE:"+f.id;
  if(f.kind==="pair") return "PAIR:"+f.id;
  return "CONTEST";
}
function archiveSnapshotData(entry){
  if(entry&&entry.whole&&entry.snapshot&&entry.snapshot.data) return entry.snapshot.data;
  return null;
}
function findArchiveFragment(entry, fragId){
  return ((entry&&entry.fragments)||[]).find(f=>f&&f.id===fragId)||null;
}
function renderArchiveNav(entry){
  const nav=$("section-nav");
  if(!nav||!entry||entry.kind!=="bundle") return;
  nav.hidden=false;
  if(!activeArchiveSection) activeArchiveSection=defaultArchiveSection(entry);
  const items=[];
  function pushGroup(cluster, title, meta, chips){
    items.push({kind:"group",group:cluster,title,meta:meta||""});
    (chips||[]).forEach(c=>items.push(c));
  }
  const d=archiveSnapshotData(entry);
  if(entry.whole&&d){
    const ct=d.contest||{};
    const typeLab=contestTypeNavLabel(ct.CONTEST_TYPE);
    const full=String(ct.FULL_NAME||"").trim();
    (()=>{
      const periodNav=contestPeriodNavItem(d);
      const items=[
        {id:"CONTEST",title:typeLab,sub:full,tag:"start",tagLabel:"DATA",slot:true,nameSlot:true,tip:full?typeLab+" · "+full:typeLab},
        {id:"CONTEST_FEATURE",title:"Особенности",sub:"CONTEST_FEATURE",tag:"json",tagLabel:"JSON",slot:true,child:true,tip:"Особенности · CONTEST_FEATURE"},
      ];
      if(periodNav) items.push(periodNav);
      pushGroup("contest","Конкурс","архив",items);
    })();
    const inds=d.indicator||[];
    if(inds.length) pushGroup("ind","Индикаторы",inds.length+" шт.", inds.flatMap((r,i)=>{
      const title=indicatorTitle(r,i); const sub=indicatorSub(r);
      const fc=indicatorFilterCount(r);
      const items=[{id:"INDICATOR:"+(i+1),title,sub,tag:"table",tagLabel:"I"+(i+1),slot:true,tip:(sub?title+" · "+sub:title)+(fc?(" · фильтры F×"+fc):"")}];
      const filt=indicatorFilterNavItem(r,i);
      if(filt) items.push(filt);
      return items;
    }));
    const grps=d.group||[];
    if(grps.length) pushGroup("groups","Группы",grps.length+" шт.", grps.map((r,i)=>{
      const title=groupTitle(r,i); const sub=groupSub(r);
      return {id:"GROUP:"+(i+1),title,sub,tag:"table",tagLabel:"G"+(i+1),slot:true,tip:sub?title+" · "+sub:title};
    }));
    const badges=d.badges||[];
    if(badges.length){
      items.push({kind:"group",group:"pair",title:"Связи + награды",meta:badges.length+" шт."});
      for(let i=0;i<badges.length;i++){
        const flat=(badges[i]&&badges[i].flat)||{};
        const code=String(flat.REWARD_CODE||"").trim();
        const label=code||("Пара "+(i+1));
        items.push({
          kind:"pairUnit",index:i,hideDelete:true,
          pairId:"PAIR:"+(i+1), addId:"ADD:"+(i+1),
          pairTitle:label, pairSub:pairTypeSub(badges[i]),
          addTitle:"Особенности", addSub:code?("JSON · "+code):"REWARD_ADD_DATA",
          tipPair:label+" · "+pairTypeSub(badges[i]), tipAdd:"JSON ADD",
        });
      }
    }
    const sch=d.schedule||[];
    if(sch.length) pushGroup("sch","Расписание",sch.length+" тур.", sch.map((r,i)=>{
      const title=scheduleTitle(r,i); const sub=scheduleSub(r);
      const badges=scheduleJsonBadges(r);
      return {id:"SCHEDULE:"+(i+1),title,sub,tag:"table",tagLabel:"T"+(i+1),slot:true,codeSlot:true,statusClass:scheduleStatusNavClass(r.TOURNAMENT_STATUS),badges,tip:(sub?title+" · "+sub:title)+(badges.length?" · "+badges.map(b=>b.text).join(" · "):"")};
    }));
  }else{
    const fr=entry.fragments||[];
    const groups=fr.filter(f=>f.kind==="group");
    const inds=fr.filter(f=>f.kind==="indicator");
    const pairs=fr.filter(f=>f.kind==="pair");
    const sch=fr.filter(f=>f.kind==="schedule");
    if(groups.length) pushGroup("groups","Группы",groups.length+" шт.", groups.map((f,i)=>{
      const r=f.payload||{}; const title=f.label||groupTitle(r,i); const sub=groupSub(r);
      return {id:"GROUP:"+f.id,title,sub,tag:"table",tagLabel:"G"+(i+1),slot:true,tip:title};
    }));
    if(inds.length) pushGroup("ind","Индикаторы",inds.length+" шт.", inds.flatMap((f,i)=>{
      const r=f.payload||{}; const title=f.label||indicatorTitle(r,i); const sub=indicatorSub(r);
      const fc=indicatorFilterCount(r);
      const navId="INDICATOR:"+f.id;
      const items=[{id:navId,title,sub,tag:"table",tagLabel:"I"+(i+1),slot:true,tip:title+(fc?(" · фильтры F×"+fc):"")}];
      const filt=indicatorFilterNavItem(r,i);
      if(filt){filt.id=navId;items.push(filt)}
      return items;
    }));
    if(pairs.length){
      items.push({kind:"group",group:"pair",title:"Связи + награды",meta:pairs.length+" шт."});
      pairs.forEach((f,i)=>{
        const p=f.payload||{};
        const flat=(p.badge&&p.badge.flat)||{};
        const code=String(flat.REWARD_CODE||(p.link&&p.link.REWARD_CODE)||"").trim();
        const label=f.label||code||("Пара "+(i+1));
        items.push({
          kind:"pairUnit",index:i,hideDelete:true,
          pairId:"PAIR:"+f.id, addId:"ADD:"+f.id,
          pairTitle:label, pairSub:pairTypeSub(p.badge),
          addTitle:"Особенности", addSub:code?("JSON · "+code):"REWARD_ADD_DATA",
          tipPair:label+" · "+pairTypeSub(p.badge), tipAdd:"JSON ADD",
        });
      });
    }
    if(sch.length) pushGroup("sch","Расписание",sch.length+" тур.", sch.map((f,i)=>{
      const r=f.payload||{}; const title=f.label||scheduleTitle(r,i); const sub=scheduleSub(r);
      const badges=scheduleJsonBadges(r);
      return {id:"SCHEDULE:"+f.id,title,sub,tag:"table",tagLabel:"T"+(i+1),slot:true,codeSlot:true,statusClass:scheduleStatusNavClass(r.TOURNAMENT_STATUS),badges,tip:title+(badges.length?" · "+badges.map(b=>b.text).join(" · "):"")};
    }));
  }
  const rowMain=[], rowLinks=[], rowSchedule=[];
  let bucket=rowMain;
  for(const it of items){
    if(it.kind==="group"){
      if(it.group==="pair") bucket=rowLinks;
      else if(it.group==="sch") bucket=rowSchedule;
      else bucket=rowMain;
    }
    bucket.push(it);
  }
  const prevActive=activeSection;
  activeSection=activeArchiveSection;
  nav.innerHTML=
    (rowMain.length?`<div class="top-nav__row top-nav__row--main">${renderNavRow(rowMain)}</div>`:"")+
    (rowLinks.length?`<div class="top-nav__row top-nav__row--links">${renderNavRow(rowLinks)}</div>`:"")+
    (rowSchedule.length?`<div class="top-nav__row top-nav__row--schedule">${renderNavRow(rowSchedule)}</div>`:"");
  activeSection=prevActive;
  nav.querySelectorAll("[data-nav]").forEach(btn=>{
    const id=btn.getAttribute("data-nav");
    btn.classList.toggle("active", id===activeArchiveSection);
    btn.addEventListener("click",e=>{
      if(e.target.closest("[data-nav-del]")) return;
      activeArchiveSection=id;
      render();
    });
  });
}
function renderArchiveView(entry){
  const ws=$("workspace");
  if(!ws||!entry||entry.kind!=="bundle") return;
  if(!activeArchiveSection) activeArchiveSection=defaultArchiveSection(entry);
  const when=formatArchiveWhen(entry.deletedAt);
  const st=archiveBundleStats(entry);
  const live=findLiveContestForArchive(entry);
  const meta=st.whole
    ?("Конкурс целиком · R:"+st.r+" · T:"+st.t+" · G:"+st.g)
    :("Части · R:"+st.r+" · T:"+st.t+" · G:"+st.g+(st.i?" · I:"+st.i:""));
  ws.innerHTML=`
<section class="panel panel--archive">
  <div class="archive-banner">
    <div>
      <h2 class="archive-banner__title">Архив · ${esc(entry.label||"Конкурс")}</h2>
      <p class="archive-banner__meta">${esc(meta)}<br>Обновлено: ${esc(when)}${st.whole?"":(live?" · конкурс в списке":" · конкурса нет в списке")}</p>
    </div>
    <div class="archive-banner__actions">
      <button type="button" class="btn btn-primary" id="btn-arch-restore" data-tip="Восстановить всё или выбранные части">Восстановить…</button>
      <button type="button" class="btn" id="btn-arch-purge" data-tip="Удалить архив этого конкурса навсегда">Удалить безвозвратно</button>
    </div>
  </div>
  <p class="intro">Только просмотр. В CSV архив не попадает. При восстановлении награды/турнира с занятым кодом — выбор: оставить, заменить или рядом.</p>
  <div id="archive-body"></div>
</section>`;
  $("btn-arch-restore")?.addEventListener("click",()=>openArchiveRestoreModal(entry));
  $("btn-arch-purge")?.addEventListener("click",()=>purgeArchiveEntry(entry));
  const body=$("archive-body");
  if(!body) return;
  const sec=activeArchiveSection;
  const d=archiveSnapshotData(entry);
  const cc=String(entry.contestCode||(d&&d.contest&&d.contest.CONTEST_CODE)||"").trim();

  function showLockedGroup(row){
    body.innerHTML=`${panelHeadHtml("Группа (архив)", [{k:"CONTEST",v:cc,tip:""}])}<div id="arch-groups"></div>`;
    appendLockedGrouped($("arch-groups"),"TABLE:GROUP",GROUP_LAYOUT,f=>f.key==="CONTEST_CODE"?cc:(row[f.key]||""),"archive.group.",["CONTEST_CODE"]);
  }
  function showLockedInd(row){
    ensureIndicatorJson(row);
    body.innerHTML=`${panelHeadHtml("Индикатор (архив)", [{k:"CONTEST",v:cc,tip:""}])}<div id="arch-groups"></div>`;
    appendLockedGrouped($("arch-groups"),"TABLE:INDICATOR",INDICATOR_LAYOUT,f=>f.key==="CONTEST_CODE"?cc:(row[f.key]||""),"archive.ind.",["CONTEST_CODE","INDICATOR_FILTER"]);
    appendJsonArrayEditor($("arch-groups"),"INDICATOR_FILTER", row.filter_items||[], {
      title:"INDICATOR_FILTER", locked:true, emptyFactory:emptyIndicatorFilterItem, pathPrefix:"archive.filter_items"
    });
  }
  function showLockedSch(row){
    ensureScheduleJson(row);
    body.innerHTML=`${panelHeadHtml("Расписание (архив)", [{k:"CONTEST",v:cc,tip:""}])}<div id="arch-groups"></div>`;
    appendLockedGrouped($("arch-groups"),"TABLE:SCHEDULE",SCHEDULE_LAYOUT,f=>f.key==="CONTEST_CODE"?cc:(row[f.key]||""),"archive.sch.",["CONTEST_CODE","TARGET_TYPE","FILTER_PERIOD_ARR"]);
    appendScheduleTargetTypeEditor($("arch-groups"), row, {locked:true});
    appendJsonArrayEditor($("arch-groups"),"FILTER_PERIOD_ARR", row.filter_period||[], {
      title:"FILTER_PERIOD_ARR", locked:true, emptyFactory:emptyFilterPeriodItem, pathPrefix:"archive.filter_period"
    });
  }
  function showLockedPair(link, badge, asAdd){
    const rc=String((badge.flat&&badge.flat.REWARD_CODE)||link.REWARD_CODE||"").trim();
    if(asAdd){
      body.innerHTML=`${panelHeadHtml("Особенности награды (архив)", [{k:"CONTEST",v:cc,tip:""},{k:"REWARD",v:rc,tip:""}])}<div id="arch-groups"></div>`;
      appendLockedGrouped($("arch-groups"),"REWARD_ADD_DATA",ADD_LAYOUT,f=>{
        const leaf=jsonStoreLeaf(f,"REWARD_ADD_DATA","ADD");
        return (badge.add&&badge.add[leaf])||"";
      },"archive.add.");
      return;
    }
    body.innerHTML=`${panelHeadHtml("Связь + награда (архив)", [{k:"CONTEST",v:cc,tip:""},{k:"REWARD",v:rc,tip:""}])}<div class="pair-block pair-block--link"><div class="pair-block__label"><span>Связь</span></div><div id="arch-link"></div></div><div class="pair-block pair-block--reward"><div class="pair-block__label"><span>Награда</span></div><div id="arch-reward"></div></div>`;
    appendLockedGrouped($("arch-link"),"TABLE:REWARD-LINK",LINK_LAYOUT,f=>f.key==="CONTEST_CODE"?cc:(link[f.key]||""),"archive.link.",["CONTEST_CODE"]);
    appendLockedGrouped($("arch-reward"),"REWARD",REWARD_LAYOUT,f=>(badge.flat&&badge.flat[f.key])||"","archive.reward.");
  }

  if(entry.whole&&d){
    const ccode=String((d.contest&&d.contest.CONTEST_CODE)||cc).trim();
    if(sec==="CONTEST"){
      body.innerHTML=`${panelHeadHtml("Конкурс (архив)", [{k:"CONTEST",v:ccode,tip:"Код из снимка"}])}<div id="arch-groups"></div>`;
      appendLockedGrouped($("arch-groups"),"CONTEST",CONTEST_LAYOUT,f=>(d.contest&&d.contest[f.key])||"","archive.contest.");
    }else if(sec==="CONTEST_FEATURE"){
      body.innerHTML=`${panelHeadHtml("Особенности (архив)", [{k:"CONTEST",v:ccode,tip:""}])}<div id="arch-groups"></div>`;
      appendLockedGrouped($("arch-groups"),"CONTEST_FEATURE",FEATURE_LAYOUT,f=>{
        const leaf=jsonStoreLeaf(f,"CONTEST_FEATURE","FEATURE");
        return (d.feature&&d.feature[leaf])||"";
      },"archive.feature.");
    }else if(sec==="CONTEST_PERIOD"){
      ensureJsonStructures(d);
      body.innerHTML=`${panelHeadHtml("Периоды (архив)", [{k:"CONTEST",v:ccode,tip:""},{k:"ARR",v:"CONTEST_PERIOD",tip:""}])}<div id="arch-groups"></div>`;
      appendJsonArrayEditor($("arch-groups"),"CONTEST_PERIOD", d.contestPeriod||[], {
        title:"CONTEST_PERIOD", locked:true, emptyFactory:emptyContestPeriodItem, pathPrefix:"archive.contestPeriod"
      });
    }else if(String(sec).startsWith("GROUP:")){
      const i=Math.max(0,Number(String(sec).split(":")[1])-1);
      showLockedGroup((d.group||[])[i]||{});
    }else if(String(sec).startsWith("INDICATOR:")){
      const i=Math.max(0,Number(String(sec).split(":")[1])-1);
      showLockedInd((d.indicator||[])[i]||{});
    }else if(String(sec).startsWith("SCHEDULE:")){
      const i=Math.max(0,Number(String(sec).split(":")[1])-1);
      showLockedSch((d.schedule||[])[i]||{});
    }else if(String(sec).startsWith("PAIR:")){
      const i=Math.max(0,Number(String(sec).split(":")[1])-1);
      showLockedPair(((d.reward_link)||[])[i]||{}, ((d.badges)||[])[i]||{flat:{},add:{}}, false);
    }else if(String(sec).startsWith("ADD:")){
      const i=Math.max(0,Number(String(sec).split(":")[1])-1);
      showLockedPair({}, ((d.badges)||[])[i]||{flat:{},add:{}}, true);
    }else body.innerHTML=`<p class="intro">Выберите раздел в верхнем колонтитуле.</p>`;
    return;
  }

  // фрагменты
  const fid=String(sec).includes(":")?String(sec).split(":").slice(1).join(":"):"";
  const frag=fid?findArchiveFragment(entry, fid):null;
  if(!frag){
    body.innerHTML=`<p class="intro">В архиве этого конкурса пока нет частей, или выберите пункт вверху.</p>`;
    return;
  }
  if(frag.kind==="group") showLockedGroup(frag.payload||{});
  else if(frag.kind==="indicator") showLockedInd(frag.payload||{});
  else if(frag.kind==="schedule") showLockedSch(frag.payload||{});
  else if(frag.kind==="pair"){
    const p=frag.payload||{};
    const asAdd=String(sec).startsWith("ADD:");
    showLockedPair(p.link||{}, p.badge||{flat:{},add:{}}, asAdd);
  }else body.innerHTML=`<p class="intro">Неизвестный тип фрагмента.</p>`;
}


function contestCodeOf(d){return String((d||data()).contest.CONTEST_CODE||"").trim()}
var ITEM_CODE_PREFIX="ITEM_";
function isItemRewardType(t){return String(t||"").trim().toUpperCase()==="ITEM"}
function contestBusinessBlockValue(d){
  d=d||data();
  const v=d&&d.contest?d.contest.BUSINESS_BLOCK:"";
  return v==null?"":v;
}
function syncBusinessBlockFromContest(d){
  d=d||data();
  if(!d||typeof d!=="object") return;
  const v=contestBusinessBlockValue(d);
  if(!d.feature) d.feature={};
  d.feature.businessBlock=v;
  for(const b of d.badges||[]){
    if(!b.add) b.add={};
    b.add.businessBlock=v;
  }
}
function lockedValueText(value){
  if(Array.isArray(value)) return value.map(x=>String(x??"").trim()).filter(Boolean).join("; ");
  return String(value??"").trim();
}
/** База без хвостового _: r_CONTEST_CODE / t_CONTEST_CODE */
function rewardCodePrefix(cc){
  const c=String(cc||"").trim();
  return c?("r_"+c):"r_";
}
function tournamentCodePrefix(cc){
  const c=String(cc||"").trim();
  return c?("t_"+c):"t_";
}
function normalizeCodeSuffix(suffix){
  return String(suffix??"").replace(/^_+/,"");
}
function followsItemPrefix(full){
  return String(full||"").trim().startsWith(ITEM_CODE_PREFIX);
}
function endingFromItemCode(full){
  const s=String(full||"").trim();
  if(s.startsWith(ITEM_CODE_PREFIX)) return s.slice(ITEM_CODE_PREFIX.length);
  return "";
}
function buildItemCode(suffix){
  return ITEM_CODE_PREFIX+normalizeCodeSuffix(suffix);
}
/**
 * Код следует правилу r_/t_ + CONTEST_CODE (+ _ + окончание).
 * Новые конкурсы — всегда так; старые PROM могут быть иначе.
 */
function followsPrefixedPrinciple(full, kind, contestCode){
  const s=String(full||"").trim();
  const cc=String(contestCode||"").trim();
  if(!s||!cc) return false;
  if(kind==="item") return followsItemPrefix(s);
  const prefix=kind==="reward"?rewardCodePrefix(cc):tournamentCodePrefix(cc);
  return s===prefix||s.startsWith(prefix+"_");
}
/** Окончание в снимке (пусто / 1 / 4001 / P1), не полный чужой код. */
function isSnapshotCodeEnding(s){
  const t=String(s||"").trim();
  if(!t) return true;
  return /^(P)?\d+$/i.test(t);
}
/**
 * Полный код: base + (окончание ? "_" + окончание : "").
 * Пустое окончание → r_CODE / t_CODE без завершающего "_".
 */
function buildPrefixedCode(prefix, suffix){
  let base=String(prefix||"");
  if(base.endsWith("_")) base=base.slice(0,-1);
  const s=normalizeCodeSuffix(suffix);
  if(!base) return s;
  if(!s) return base;
  return base+"_"+s;
}
/** Вытащить окончание: снять r_/t_, снять CONTEST_CODE, остаток без ведущих _. */
function endingFromFullCode(full, contestCode, kind){
  let s=String(full||"").trim();
  const cc=String(contestCode||"").trim();
  if(kind==="item") return endingFromItemCode(s);
  if(kind==="reward"&&s.startsWith("r_")) s=s.slice(2);
  else if(kind==="tournament"&&s.startsWith("t_")) s=s.slice(2);
  if(cc&&s.startsWith(cc)) s=s.slice(cc.length);
  return normalizeCodeSuffix(s);
}
function rewardEndingOf(full, contestCode, rewardType){
  const s=String(full||"").trim();
  if(isItemRewardType(rewardType)){
    return followsItemPrefix(s)?endingFromItemCode(s):"";
  }
  const cc=String(contestCode||"").trim();
  if(followsPrefixedPrinciple(s, "reward", cc)) return endingFromFullCode(s, cc, "reward");
  return "";
}
function tournamentEndingOf(full, contestCode){
  const s=String(full||"").trim();
  const cc=String(contestCode||"").trim();
  if(followsPrefixedPrinciple(s, "tournament", cc)) return endingFromFullCode(s, cc, "tournament");
  return "";
}
function expandStoredItemCode(raw){
  const s=String(raw||"").trim();
  if(!s) return buildItemCode("");
  if(followsItemPrefix(s)) return s;
  if(isSnapshotCodeEnding(s)) return buildItemCode(s);
  return s;
}
/** Значение из снимка: окончание или полный код → полный код fill/SPOD. */
function expandStoredPrefixedCode(raw, kind, contestCode){
  const cc=String(contestCode||"").trim();
  const prefix=kind==="reward"?rewardCodePrefix(cc):tournamentCodePrefix(cc);
  const s=String(raw||"").trim();
  if(kind==="reward"&&followsItemPrefix(s)) return s;
  if(!s) return cc?buildPrefixedCode(prefix, ""):"";
  if(followsPrefixedPrinciple(s, kind, cc)) return s;
  if(/^[rt]_/i.test(s)) return s;
  if(isSnapshotCodeEnding(s)) return buildPrefixedCode(prefix, s);
  return s;
}
function expandStoredRewardCode(raw, contestCode, rewardType){
  if(isItemRewardType(rewardType)) return expandStoredItemCode(raw);
  return expandStoredPrefixedCode(raw, "reward", contestCode);
}
function convertRewardCodeForType(full, prevType, nextType, contestCode){
  const s=String(full||"").trim();
  const cc=String(contestCode||"").trim();
  const wasItem=isItemRewardType(prevType);
  const nowItem=isItemRewardType(nextType);
  if(wasItem===nowItem) return s;
  if(!wasItem&&nowItem){
    if(followsPrefixedPrinciple(s, "reward", cc)) return buildItemCode(endingFromFullCode(s, cc, "reward"));
    return s;
  }
  if(wasItem&&!nowItem){
    if(followsItemPrefix(s)) return buildPrefixedCode(rewardCodePrefix(cc), endingFromItemCode(s));
    return s;
  }
  return s;
}
function pairRewardTypeOf(d, idx){
  const b=(d&&d.badges||[])[idx];
  return String((b&&b.flat&&b.flat.REWARD_TYPE)||"").trim();
}
function applyRewardCodeToPair(d, idx, full){
  const cc=contestCodeOf(d);
  const rt=pairRewardTypeOf(d, idx);
  const code=String(full||"").trim();
  const ending=rewardEndingOf(code, cc, rt);
  const link=(d.reward_link||[])[idx];
  const badge=(d.badges||[])[idx];
  if(link){
    link.REWARD_CODE=code;
    link.REWARD_CODE_ENDING=ending;
  }
  if(badge){
    if(!badge.flat) badge.flat={};
    badge.flat.REWARD_CODE=code;
    badge.flat.REWARD_CODE_ENDING=ending;
  }
}
function applyTournamentCodeToRow(row, full, contestCode){
  const cc=contestCode==null?contestCodeOf():contestCode;
  const code=String(full||"").trim();
  row.TOURNAMENT_CODE=code;
  row.TOURNAMENT_CODE_ENDING=tournamentEndingOf(code, cc);
}
function isBlankCell(v){
  if(v==null) return true;
  if(Array.isArray(v)) return v.length===0;
  if(typeof v==="object") return Object.keys(v).length===0;
  return String(v).trim()==="";
}
/** Строка-заглушка: кроме CONTEST_CODE все значимые поля пустые. */
function isStubTableRow(row, extraSkip){
  if(!row||typeof row!=="object") return true;
  const skip=new Set(["CONTEST_CODE"].concat(extraSkip||[]));
  for(const [k,v] of Object.entries(row)){
    if(skip.has(k)) continue;
    if(!isBlankCell(v)) return false;
  }
  return true;
}
function isStubBadge(b){
  if(!b||typeof b!=="object") return true;
  const flat=b.flat||{};
  const add=b.add||{};
  const skip=new Set(["REWARD_TYPE"]);
  for(const [k,v] of Object.entries(flat)){
    if(skip.has(k)) continue;
    if(!isBlankCell(v)) return false;
  }
  for(const v of Object.values(add)){
    if(!isBlankCell(v)) return false;
  }
  return true;
}
function isStubLink(row){
  if(!row||typeof row!=="object") return true;
  return isBlankCell(row.REWARD_CODE)&&isBlankCell(row.GROUP_CODE);
}
/** Убрать фантомные пустые строки, которых не было в CSV. */
function pruneImportedEmptyRows(d){
  if(!d||typeof d!=="object") return;
  if(Array.isArray(d.schedule)) d.schedule=d.schedule.filter(r=>!isStubTableRow(r,["seasonCode","filter_period","TARGET_TYPE","FILTER_PERIOD_ARR"]));
  if(Array.isArray(d.indicator)) d.indicator=d.indicator.filter(r=>!isStubTableRow(r,["filter_items","INDICATOR_FILTER"]));
  if(Array.isArray(d.group)) d.group=d.group.filter(r=>!isStubTableRow(r));
  if(Array.isArray(d.badges)&&Array.isArray(d.reward_link)){
    const nextB=[];const nextL=[];
    const n=Math.max(d.badges.length,d.reward_link.length);
    for(let i=0;i<n;i++){
      const b=d.badges[i];
      const l=d.reward_link[i];
      if(isStubBadge(b||{})&&isStubLink(l||{})) continue;
      if(b) nextB.push(b);
      if(l) nextL.push(l);
    }
    d.badges=nextB;
    d.reward_link=nextL;
  }
}

function expandContestPrefixedCodes(d){
  const cc=contestCodeOf(d);
  const n=Math.max((d.reward_link||[]).length,(d.badges||[]).length);
  for(let i=0;i<n;i++){
    const link=(d.reward_link||[])[i];
    const badge=(d.badges||[])[i];
    const rt=String((badge&&badge.flat&&badge.flat.REWARD_TYPE)||"").trim();
    const raw=String((link&&link.REWARD_CODE)||(badge&&badge.flat&&badge.flat.REWARD_CODE)||"").trim();
    const full=expandStoredRewardCode(raw, cc, rt);
    applyRewardCodeToPair(d, i, full);
    if(badge&&badge.add&&badge.add.parentRewardCode){
      const p=String(badge.add.parentRewardCode||"").trim();
      if(p){
        if(followsItemPrefix(p)) badge.add.parentRewardCode=p;
        else badge.add.parentRewardCode=expandStoredPrefixedCode(p, "reward", cc);
      }
    }
  }
  for(const row of d.schedule||[]){
    const full=expandStoredPrefixedCode(row.TOURNAMENT_CODE, "tournament", cc);
    applyTournamentCodeToRow(row, full, cc);
  }
  syncBusinessBlockFromContest(d);
}
/** Отрезать базу префикса от полного кода → окончание ("" если хвоста нет). */
function suffixAfterPrefix(full, prefix){
  const s=String(full||"");
  let base=String(prefix||"");
  if(!s||!base) return null;
  if(base.endsWith("_")) base=base.slice(0,-1);
  if(s===base||s===base+"_") return "";
  if(s.startsWith(base+"_")) return normalizeCodeSuffix(s.slice(base.length+1));
  return null;
}
/** Вытащить окончание из полного кода (с учётом смены CONTEST_CODE). */
function extractCodeSuffix(full, prefix, prevPrefix){
  const s=String(full||"");
  if(!s) return "";
  let cut=suffixAfterPrefix(s, prefix);
  if(cut!==null) return cut;
  cut=suffixAfterPrefix(s, prevPrefix);
  if(cut!==null) return cut;
  // r_CODE / t_CODE без суффикса или r_CODE_suf
  if(s.startsWith("r_")||s.startsWith("t_")){
    const kind=s.startsWith("r_")?"reward":"tournament";
    const cc=contestCodeOf();
    return endingFromFullCode(s, cc, kind);
  }
  return normalizeCodeSuffix(s);
}
function resyncPrefixedCodes(d, prevCc){
  const cc=contestCodeOf(d);
  const rPrev=rewardCodePrefix(prevCc);
  const rNow=rewardCodePrefix(cc);
  const tPrev=tournamentCodePrefix(prevCc);
  const tNow=tournamentCodePrefix(cc);
  function rewrite(full, kind, nowP, prevP){
    const s=String(full||"").trim();
    if(kind==="reward"&&followsItemPrefix(s)) return s;
    if(!s) return buildPrefixedCode(nowP, "");
    if(followsPrefixedPrinciple(s, kind, cc)) return s;
    if(prevCc && followsPrefixedPrinciple(s, kind, prevCc)){
      return buildPrefixedCode(nowP, extractCodeSuffix(s, nowP, prevP));
    }
    return s;
  }
  for(let i=0;i<(d.reward_link||[]).length;i++){
    const rt=pairRewardTypeOf(d, i);
    if(isItemRewardType(rt)) continue;
    const row=d.reward_link[i];
    applyRewardCodeToPair(d, i, rewrite(row.REWARD_CODE, "reward", rNow, rPrev));
  }
  for(const row of d.schedule||[]){
    applyTournamentCodeToRow(row, rewrite(row.TOURNAMENT_CODE, "tournament", tNow, tPrev), cc);
  }
  const allowed=new Set(linkRewardCodes(d));
  for(let i=0;i<(d.badges||[]).length;i++){
    const b=d.badges[i];
    const rc=String((b.flat&&b.flat.REWARD_CODE)||"").trim();
    if(isItemRewardType(pairRewardTypeOf(d, i))) continue;
    if(rc && !allowed.has(rc)){
      const next=rewrite(rc, "reward", rNow, rPrev);
      applyRewardCodeToPair(d, i, allowed.has(next)?next:(allowed.size===1?[...allowed][0]:rc));
    }
  }
}
function linkRewardCodes(d){
  d=d||data();
  const seen=new Set();const out=[];
  for(const row of d.reward_link||[]){
    const rc=String(row.REWARD_CODE||"").trim();
    if(!rc||seen.has(rc))continue;
    seen.add(rc);out.push(rc);
  }
  return out;
}
function groupCodesOf(d){
  d=d||data();
  const seen=new Set();const out=[];
  for(const row of d.group||[]){
    const gc=String(row.GROUP_CODE||"").trim();
    if(!gc||seen.has(gc))continue;
    seen.add(gc);out.push(gc);
  }
  return out;
}
function emptyLinkRow(){
  const row=Object.fromEntries(LINK_COLS.map(c=>[c,""]));
  row.CONTEST_CODE=contestCodeOf();
  for(const f of (catalog.sections.find(s=>s.id==="TABLE:REWARD-LINK")||{fields:[]}).fields){
    if(f.key==="CONTEST_CODE"||f.key==="REWARD_CODE") continue;
    if(f.default!=null&&String(f.default)!==""&&!row[f.key]) row[f.key]=String(f.default);
  }
  return tagNewEntityStands(row);
}
function pruneLinkGroupCodes(d){
  const allowed=new Set(groupCodesOf(d));
  for(const row of d.reward_link||[]){
    const gc=String(row.GROUP_CODE||"").trim();
    if(gc&&!allowed.has(gc)) row.GROUP_CODE="";
  }
}
function ensureBadgeTypes(d){
  for(const b of d.badges||[]){if(!b.flat.REWARD_TYPE)b.flat.REWARD_TYPE="BADGE"}
}
function linkTitle(row,i){
  const rc=row.REWARD_CODE||"";
  const gc=row.GROUP_CODE||"";
  return "Связь "+(i+1)+(rc?" · "+rc:"")+(gc?" · "+gc:"");
}
function seedLinked(data){
  ensureJsonStructures(data);
  const code=String(data.contest.CONTEST_CODE||"").trim();
  for(const row of data.reward_link||[])row.CONTEST_CODE=code;
  for(const row of data.group||[])row.CONTEST_CODE=code;
  for(const row of data.indicator||[])row.CONTEST_CODE=code;
  for(const row of data.schedule||[])row.CONTEST_CODE=code;
  pruneLinkGroupCodes(data);
  ensureBadgeTypes(data);
}

function syncBadgeSlots(data,force){
  const rec=recommendedBadges(data.contest.CONTEST_TYPE);
  if(!Array.isArray(data.badges)) data.badges=[];
  if(force){
    // При смене типа: добить до рекомендации, лишние пары не удалять
    while(data.badges.length<rec) data.badges.push(emptyBadge());
  }
  const n=data.badges.length;
  const cc=String(data.contest.CONTEST_CODE||"").trim();
  const links=[];
  for(let i=0;i<n;i++){
    const prev=(data.reward_link&&data.reward_link[i])?Object.assign({},data.reward_link[i]):Object.fromEntries(LINK_COLS.map(c=>[c,""]));
    prev.CONTEST_CODE=cc;
    links.push(prev);
  }
  data.reward_link=links;
  data.badges.forEach((b,i)=>{
    if(!b.flat.REWARD_TYPE)b.flat.REWARD_TYPE="BADGE";
    b.flat.REWARD_CODE=String((data.reward_link[i]||{}).REWARD_CODE||"");
  });
  if(activeLink>=n) activeLink=Math.max(0,n-1);
  activeBadge=activeLink;
  seedLinked(data);
}

function cur(){return contests[activeContest]}
function data(){return cur().data}
function baseline(){return cur().baseline}
function fingerprint(d){return JSON.stringify(d)}
/** Оранжевая обводка только после правок пользователя, не из автонормализации. */
function isContestDirty(c){return !!(c&&c.userEdited)}
/** >0 — идёт автоподстановка дефолтов при монтировании полей; dirty не ставим. */
var suppressDirtyMark=0;
function withSilentNormalize(fn){
  suppressDirtyMark++;
  try{return fn()}
  finally{suppressDirtyMark--}
}
function markBaseline(c){
  if(!c)return;
  c.baseline=clone(c.data);
  c.userEdited=false;
}
/** Подтянуть baseline после автосид/сортировки, если пользователь ещё не правил. */
function realignBaselineIfPristine(c){
  if(!c||c.userEdited) return;
  markBaseline(c);
}
/** data отличается от baseline (есть что выделять в списке). */
function contestDiffersFromBaseline(c){
  if(!c||!c.baseline) return false;
  return fingerprint(c.data)!==fingerprint(c.baseline);
}
/** Обновить userEdited и обводку: снять, если всё вернули как в baseline. */
function syncContestDirtyState(contestIndex){
  if(suppressDirtyMark>0) return;
  const i=contestIndex==null?activeContest:contestIndex;
  const c=contests[i];
  if(!c) return;
  if(!c.baseline){
    markBaseline(c);
    return;
  }
  const dirty=contestDiffersFromBaseline(c);
  c.userEdited=dirty;
  const host=$("contest-tabs");
  if(!host) return;
  host.querySelectorAll(`.contest-tab[data-ci="${i}"]:not([data-pair])`).forEach(el=>el.classList.toggle("is-dirty", dirty));
}
function markContestEdited(){
  syncContestDirtyState(activeContest);
}
function refreshContestTabDirty(){
  syncContestDirtyState(activeContest);
}

function closeAllDatePops(){
  document.querySelectorAll(".date-pop").forEach(p=>{p.hidden=true;p.classList.remove("is-open");if(p.parentNode)p.parentNode.removeChild(p)});
  document.querySelectorAll(".default-date.is-picker-open").forEach(w=>w.classList.remove("is-picker-open"));
  window.__spodActiveDateWrap=null;
}

function mountDateUi(host, value, onChange, tip){
  const wrap=document.createElement("div");wrap.className="default-date";
  const row=document.createElement("div");row.className="default-date__row";
  const fieldWrap=document.createElement("div");fieldWrap.className="default-date__field";
  const text=document.createElement("input");text.type="text";text.className="default-date__iso";text.placeholder="YYYY-MM-DD";text.value=isIsoDate(value)?value:"";text.setAttribute("data-tip",tip||"Дата YYYY-MM-DD");
  const calBtn=document.createElement("button");calBtn.type="button";calBtn.className="default-date__cal";calBtn.innerHTML=calSvg();calBtn.setAttribute("data-tip","Календарь");
  const pop=document.createElement("div");pop.className="date-pop";pop.hidden=true;pop.setAttribute("role","dialog");
  const presets=document.createElement("div");presets.className="default-date__presets";
  const view={y:new Date().getFullYear(),m:new Date().getMonth()};
  let current=isIsoDate(value)?value:"";

  function syncPreset(){
    presets.querySelectorAll(".default-date__chip").forEach(btn=>{
      const id=btn.getAttribute("data-preset");
      let match=false;
      if(id==="year-start")match=current===dateYearStart();
      else if(id==="year-end")match=current===dateYearEnd();
      else if(id==="infinite")match=current===DATE_INFINITE;
      btn.classList.toggle("is-on",match);
    });
  }
  function closePop(){pop.hidden=true;pop.classList.remove("is-open");wrap.classList.remove("is-picker-open");if(window.__spodActiveDateWrap===wrap)window.__spodActiveDateWrap=null}
  function placePop(){const rect=fieldWrap.getBoundingClientRect();const width=Math.min(320,Math.max(280,innerWidth-16));let left=rect.left;if(left+width>innerWidth-8)left=Math.max(8,innerWidth-8-width);pop.style.width=width+"px";pop.style.left=Math.round(left)+"px";pop.style.top=Math.round(rect.bottom+6)+"px"}
  function applyDate(next,{close=true}={}){current=isIsoDate(next)?next:"";text.value=current;syncPreset();onChange(current);if(close)closePop();else if(!pop.hidden)paintPop()}
  function paintPop(){
    const selected=current;const first=new Date(view.y,view.m,1);let startDow=first.getDay();startDow=startDow===0?6:startDow-1;const daysInMonth=new Date(view.y,view.m+1,0).getDate();
    const mStart=dateMonthStart(view.y,view.m);const mEnd=dateMonthEnd(view.y,view.m);const today=dateToday();
    let html=`<div class="date-pop__head"><button type="button" class="date-pop__nav" data-nav="-1">‹</button><div class="date-pop__title">${MONTHS[view.m]} ${view.y}</div><button type="button" class="date-pop__nav" data-nav="1">›</button></div><div class="date-pop__dow">${DOW.map(d=>`<span>${d}</span>`).join("")}</div><div class="date-pop__grid">`;
    for(let i=0;i<startDow;i++)html+=`<span class="date-pop__empty"></span>`;
    for(let day=1;day<=daysInMonth;day++){const iso=`${view.y}-${String(view.m+1).padStart(2,"0")}-${String(day).padStart(2,"0")}`;const on=iso===selected?" is-on":"";const todayIso=today;const isTod=iso===todayIso?" is-today":"";html+=`<button type="button" class="date-pop__day${on}${isTod}" data-day="${iso}">${day}</button>`}
    html+=`</div><div class="date-pop__quick" role="group">`+
      `<button type="button" class="date-pop__qchip${selected===mStart?" is-on":""}" data-q="month-start" data-tip="Первый день открытого месяца (${mStart})"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 12h16"/><path d="M8 8l-4 4 4 4"/></svg>Нач. мес.</button>`+
      `<button type="button" class="date-pop__qchip${selected===mEnd?" is-on":""}" data-q="month-end" data-tip="Последний день открытого месяца (${mEnd})"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 12h16"/><path d="M16 8l4 4-4 4"/></svg>Кон. мес.</button>`+
      `<button type="button" class="date-pop__qchip${selected===today?" is-on":""}" data-q="today" data-tip="Сегодня (${today})"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="8"/><path d="M12 8v4l2 2"/></svg>Сегодня</button>`+
      `</div>`;
    pop.innerHTML=html;
    pop.querySelectorAll("[data-nav]").forEach(btn=>btn.addEventListener("click",e=>{e.preventDefault();e.stopPropagation();view.m+=Number(btn.getAttribute("data-nav")||0);if(view.m<0){view.m=11;view.y-=1}else if(view.m>11){view.m=0;view.y+=1}paintPop()}));
    pop.querySelectorAll("[data-day]").forEach(btn=>btn.addEventListener("click",e=>{e.preventDefault();e.stopPropagation();applyDate(btn.getAttribute("data-day")||"")}));
    pop.querySelectorAll("[data-q]").forEach(btn=>btn.addEventListener("click",e=>{
      e.preventDefault();e.stopPropagation();
      const q=btn.getAttribute("data-q");
      let next="";
      if(q==="month-start") next=dateMonthStart(view.y,view.m);
      else if(q==="month-end") next=dateMonthEnd(view.y,view.m);
      else if(q==="today"){next=dateToday();const d=new Date();view.y=d.getFullYear();view.m=d.getMonth()}
      applyDate(next,{close:true});
    }));
  }
  function openPop(){
    if(isIsoDate(current)){const[yy,mm]=current.split("-").map(Number);view.y=yy;view.m=mm-1}
    document.querySelectorAll(".date-pop").forEach(p=>{if(p!==pop){p.hidden=true;p.classList.remove("is-open")}});
    if(!pop.isConnected)document.body.appendChild(pop);
    paintPop();pop.hidden=false;pop.classList.add("is-open");wrap.classList.add("is-picker-open");window.__spodActiveDateWrap=wrap;placePop();
  }
  const defs=[
    {id:"year-start",label:"Начало года",tip:"1 января текущего года",value:dateYearStart,icon:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M8 14h3"/></svg>'},
    {id:"year-end",label:"Конец года",tip:"31 декабря текущего года",value:dateYearEnd,icon:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M13 14h3"/></svg>'},
    {id:"infinite",label:"Бесконечный",tip:"4000-01-01",value:()=>DATE_INFINITE,icon:'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18.2 8.2a4.2 4.2 0 0 0-6 0L12 8.4l-.2-.2a4.2 4.2 0 1 0 0 6l.2.2.2-.2a4.2 4.2 0 0 0 6-6z"/><path d="M5.8 8.2a4.2 4.2 0 0 1 6 0L12 8.4l.2-.2a4.2 4.2 0 1 1 0 6l-.2.2-.2-.2a4.2 4.2 0 0 1-6-6z"/></svg>'},
  ];
  for(const p of defs){const btn=document.createElement("button");btn.type="button";btn.className="default-date__chip";btn.setAttribute("data-preset",p.id);btn.setAttribute("data-tip",p.tip);btn.innerHTML=`<span class="default-date__chip-icon">${p.icon}</span><span class="default-date__chip-label">${esc(p.label)}</span>`;btn.addEventListener("click",()=>{const next=p.value();applyDate(current===next?"":next,{close:true})});presets.appendChild(btn)}
  text.addEventListener("change",()=>{const v=text.value.trim();if(!v){applyDate("",{close:false});return}if(!isIsoDate(v)){toast("Дата: YYYY-MM-DD");text.value=current;return}applyDate(v,{close:true})});
  text.addEventListener("focus",()=>{if(!pop.hidden)closePop();host.closest(".field")?.classList.add("is-focus")});
  text.addEventListener("blur",()=>host.closest(".field")?.classList.remove("is-focus"));
  calBtn.addEventListener("click",e=>{e.preventDefault();e.stopPropagation();if(!pop.hidden&&window.__spodActiveDateWrap===wrap)closePop();else openPop()});
  fieldWrap.appendChild(text);fieldWrap.appendChild(calBtn);document.body.appendChild(pop);row.appendChild(fieldWrap);row.appendChild(presets);wrap.appendChild(row);host.appendChild(wrap);syncPreset();
}

function listFromCell(raw){
  return String(raw||"").split(/[;\n]+/).map(x=>x.trim()).filter(x=>x!=="");
}
function listToCell(items){
  return (Array.isArray(items)?items:listFromCell(items)).join("\n");
}
function selectionRequired(f){const kind=f.kind||"text";return !f.allow_empty&&(kind==="dropdown"||kind==="dropdown_custom"||kind==="list")}

/** Свободный массив: одна строка = один элемент (разделитель — перевод строки). */
function mountListFreeform(host, f, value, onChange){
  const required=selectionRequired(f);
  const wrap=document.createElement("div");
  wrap.className="list-freeform";
  wrap.setAttribute("data-tip",tipFor(f)+"\nМассив: каждый элемент с новой строки");
  const ta=document.createElement("textarea");
  ta.value=listToCell(value||"");
  ta.placeholder="каждый элемент с новой строки…";
  ta.rows=Math.max(3, listFromCell(value||"").length||3);
  ta.addEventListener("input",()=>onChange(listToCell(ta.value)));
  ta.addEventListener("change",()=>{
    let items=listFromCell(ta.value);
    if(required&&!items.length){toast("«можно не указывать» = нет — требуется указать значение");ta.value=listToCell(value||"");return}
    const next=listToCell(items);
    ta.value=next;
    onChange(next);
  });
  ta.addEventListener("focus",()=>host.closest(".field")?.classList.add("is-focus"));
  ta.addEventListener("blur",()=>host.closest(".field")?.classList.remove("is-focus"));
  wrap.appendChild(ta);
  host.appendChild(wrap);
}

function mountChips(host, f, value, onChange, {multi}){
  const box=document.createElement("div");box.className="default-checks"+(multi?"":" default-checks--single");
  const variants=f.variants||[];
  const required=selectionRequired(f);
  let curVal=value||"";
  // Не подставляем значение за пользователя: только подсветка «нужно выбрать»
  const needPick=required&&variants.length&&(
    multi?!listFromCell(curVal).filter(x=>variants.includes(x)).length
         :(!String(curVal).trim()||!variants.includes(String(curVal)))
  );
  if(needPick) box.classList.add("is-need-pick");
  box.setAttribute("data-tip",tipFor(f)+(required?"\nТребуется указать: нельзя снять все варианты (выбор за вас не делается)":"\nМожно не указывать: повторный клик снимает выбор"));
  const extra=multi?document.createElement("input"):null;
  if(extra){extra.type="text";extra.placeholder="дополнительно (строки или ;)";extra.value=listToCell(curVal);extra.style.marginTop="6px";extra.addEventListener("change",()=>{
    let next=listToCell(extra.value);
    if(required){const parts=listFromCell(next);if(!parts.length){toast("«можно не указывать» = нет — требуется указать значение");extra.value=curVal;return}}
    curVal=next;onChange(curVal);paint()
  })}
  function selected(){return multi?listFromCell(curVal):[String(curVal||"")].filter(Boolean)}
  function paint(){
    const sel=new Set(selected());
    box.innerHTML="";
    variants.forEach(v=>{
      const btn=document.createElement("button");btn.type="button";btn.className="default-chip"+(sel.has(v)?" is-on":"");
      const lab=labelForVariant(f,v);
      btn.setAttribute("data-tip", lab?`${lab} → в файл: ${v}`:`В файл: ${v}`);
      btn.innerHTML=`<span class="default-chip__mark">${markSvg()}</span>${chipFaceHtml(v,lab)}`;
      btn.addEventListener("click",()=>{
        if(multi){
          let cur=listFromCell(curVal);
          if(cur.includes(v)){
            if(required&&cur.length<=1){toast("«можно не указывать» = нет — оставьте хотя бы один");return}
            cur=cur.filter(x=>x!==v);
          }else cur.push(v);
          curVal=listToCell(cur);if(extra)extra.value=curVal;onChange(curVal);paint();refreshContestTabDirty();
          box.classList.toggle("is-need-pick", required&&!listFromCell(curVal).length);
        }else{
          if(curVal===v){
            if(required){toast("«можно не указывать» = нет — требуется указать значение");return}
            curVal="";
          }else curVal=v;
          onChange(curVal);paint();afterFieldKey(f.key);
          box.classList.toggle("is-need-pick", required&&!String(curVal||"").trim());
          // Полный render только когда меняется структура (тип → число наград)
          if(f.key==="CONTEST_TYPE") render();
          else refreshContestTabDirty();
        }
      });
      box.appendChild(btn);
    });
  }
  host.appendChild(box);if(extra)host.appendChild(extra);paint();
}

/** Группа кода для combobox: хвост VKS/VKO или префикс до «_». */
function variantGroupKey(code){
  const s=String(code||"");
  if(/_VKS$/.test(s)) return "KANBANARS · VKS";
  if(/_VKO$/.test(s)) return "KANBANARS · VKO";
  const i=s.indexOf("_");
  if(i<=0) return "Прочее";
  return s.slice(0,i);
}
function mountSearchCombobox(host, f, value, onChange){
  const variants=(f.variants||[]).map(v=>String(v));
  const required=selectionRequired(f);
  let curVal=String(value||"");
  if(required&&variants.length&&(!curVal.trim()||!variants.includes(curVal))){
    curVal=String(f.default||variants[0]||"");
    withSilentNormalize(()=>onChange(curVal));
  }
  const wrap=document.createElement("div");
  wrap.className="combo";
  wrap.setAttribute("data-tip",tipFor(f)+"\nПоиск по коду. Свой вариант нельзя.");
  const inp=document.createElement("input");
  inp.type="search";
  inp.className="combo__input";
  inp.autocomplete="off";
  inp.placeholder="Найти код…";
  const lab0=labelForVariant(f,curVal);
  inp.value=lab0&&lab0!==curVal?(lab0+" · "+curVal):curVal;
  const list=document.createElement("div");
  list.className="combo__list";
  list.hidden=true;
  let hi=-1;
  let shown=[];
  function close(){list.hidden=true;hi=-1}
  function grouped(q){
    const needle=String(q||"").trim().toLowerCase();
    const out=[];
    const map=new Map();
    variants.forEach(v=>{
      const lab=labelForVariant(f,v);
      const hay=(v+" "+lab).toLowerCase();
      if(needle&&!hay.includes(needle)) return;
      const g=variantGroupKey(v);
      if(!map.has(g)) map.set(g,[]);
      map.get(g).push(v);
    });
    for(const [g,arr] of map) out.push({g,arr});
    return out;
  }
  function paint(q){
    const groups=grouped(q);
    shown=groups.flatMap(x=>x.arr);
    if(!shown.length){
      list.innerHTML=`<div class="combo__empty">Нет совпадений</div>`;
      return;
    }
    let html="";
    groups.forEach(gr=>{
      html+=`<div class="combo__group">${esc(gr.g)}</div>`;
      gr.arr.forEach(v=>{
        const lab=labelForVariant(f,v);
        const on=v===curVal?" is-on":"";
        html+=`<button type="button" class="combo__opt${on}" data-v="${esc(v)}">${esc(lab&&lab!==v?lab+" · "+v:v)}</button>`;
      });
    });
    list.innerHTML=html;
    list.querySelectorAll("[data-v]").forEach(btn=>{
      btn.addEventListener("mousedown",e=>e.preventDefault());
      btn.addEventListener("click",()=>pick(btn.getAttribute("data-v")));
    });
  }
  function pick(v){
    curVal=String(v||"");
    const lab=labelForVariant(f,curVal);
    inp.value=lab&&lab!==curVal?(lab+" · "+curVal):curVal;
    onChange(curVal);
    close();
    afterFieldKey(f.key);
    refreshContestTabDirty();
  }
  function open(){
    paint(inp.value===curVal||inp.value.indexOf(" · ")>=0?"":inp.value);
    list.hidden=false;
  }
  inp.addEventListener("focus",()=>open());
  inp.addEventListener("input",()=>{open();paint(inp.value)});
  inp.addEventListener("blur",()=>{
    setTimeout(()=>{
      if(!variants.includes(curVal)&&required&&variants.length){
        curVal=String(f.default||variants[0]);
        onChange(curVal);
      }
      const lab=labelForVariant(f,curVal);
      inp.value=lab&&lab!==curVal?(lab+" · "+curVal):curVal;
      close();
    },120);
  });
  inp.addEventListener("keydown",e=>{
    if(e.key==="Escape"){close();inp.blur();return}
    if(e.key==="ArrowDown"||e.key==="ArrowUp"){
      e.preventDefault();
      if(list.hidden) open();
      if(!shown.length) return;
      if(e.key==="ArrowDown") hi=Math.min(shown.length-1,hi+1);
      else hi=Math.max(0,hi<0?shown.length-1:hi-1);
      list.querySelectorAll(".combo__opt").forEach((el,i)=>el.classList.toggle("is-hi",i===hi));
      const el=list.querySelectorAll(".combo__opt")[hi];
      if(el) el.scrollIntoView({block:"nearest"});
      return;
    }
    if(e.key==="Enter"){
      e.preventDefault();
      if(hi>=0&&shown[hi]) pick(shown[hi]);
    }
  });
  wrap.appendChild(inp);
  wrap.appendChild(list);
  host.appendChild(wrap);
}

/** Сопоставить значение списка+ с чипом или «своим вариантом» (импорт). */
function resolveDropdownCustomValue(f, value){
  const variants=(f.variants||[]).map(v=>String(v));
  const labels=f.variant_labels||[];
  const raw=value==null?"":String(value);
  const t=raw.trim();
  if(!t) return {mode:"empty", value:"", chip:null, custom:""};
  for(const v of variants){
    if(v===raw || String(v).trim()===t) return {mode:"chip", value:v, chip:v, custom:""};
  }
  for(let i=0;i<variants.length;i++){
    const lab=String(labels[i]||"").trim();
    if(lab && (lab===t || lab===raw)) return {mode:"chip", value:variants[i], chip:variants[i], custom:""};
  }
  return {mode:"custom", value:raw, chip:null, custom:raw};
}

/** Dropdown с полем «свой вариант»: в сохранение идёт выбранный чип или текст своего ввода.
 *  Если при импорте значения нет в списке — оно показывается в «Свой вариант». */
function mountDropdownCustom(host, f, value, onChange){
  const variants=(f.variants||[]).map(v=>String(v));
  const required=selectionRequired(f);
  let resolved=resolveDropdownCustomValue(f, value);
  let curVal=resolved.value;
  if(required&&resolved.mode==="empty"&&variants.length){
    curVal=variants[0];
    resolved={mode:"chip", value:curVal, chip:curVal, custom:""};
    withSilentNormalize(()=>onChange(curVal));
  }else if(resolved.mode==="chip"&&curVal!==String(value??"")){
    // нормализация trim / подпись→код без пометки dirty
    withSilentNormalize(()=>onChange(curVal));
  }
  const wrap=document.createElement("div");
  wrap.setAttribute("data-tip",tipFor(f)+(required?"\nТребуется указать: чип или свой текст — пусто нельзя":"\nМожно не указывать: можно снять чип и очистить свой вариант"));
  const box=document.createElement("div");box.className="default-checks default-checks--single";
  const custom=document.createElement("div");custom.className="custom-variant";
  custom.innerHTML=`<div class="custom-variant__label">Свой вариант</div>`;
  const input=document.createElement("input");input.type="text";input.placeholder="Введите своё значение…";
  input.setAttribute("data-tip","Если значения нет в списке (в т.ч. после импорта) — оно хранится здесь и уходит в CSV/JSON");
  custom.appendChild(input);

  function isCustom(v){return resolveDropdownCustomValue(f,v).mode==="custom"}
  function paintChips(){
    const r=resolveDropdownCustomValue(f, curVal);
    const sel=new Set(r.chip?[r.chip]:[]);
    box.innerHTML="";
    variants.forEach(v=>{
      const btn=document.createElement("button");btn.type="button";btn.className="default-chip"+(sel.has(v)?" is-on":"");
      const lab=labelForVariant(f,v);
      btn.setAttribute("data-tip", lab?`${lab} → в файл: ${v}`:`В файл: ${v}`);
      btn.innerHTML=`<span class="default-chip__mark">${markSvg()}</span>${chipFaceHtml(v,lab)}`;
      btn.addEventListener("click",()=>{
        if(curVal===v){
          if(required){toast("«можно не указывать» = нет — оставьте чип или введите свой вариант");return}
          curVal="";
          input.value="";
        }else{
          curVal=v;
          input.value="";
        }
        onChange(curVal);
        paintChips();
        custom.classList.toggle("is-on",isCustom(curVal));
        afterFieldKey(f.key);
      });
      box.appendChild(btn);
    });
    custom.classList.toggle("is-on",isCustom(curVal));
  }
  // Импорт / неизвестное значение → сразу в «Свой вариант»
  if(resolved.mode==="custom"){
    input.value=resolved.custom;
    custom.classList.add("is-on");
  }else{
    input.value="";
  }
  input.addEventListener("input",()=>{
    curVal=input.value;
    onChange(curVal);
    paintChips();
  });
  input.addEventListener("change",()=>{
    curVal=input.value.trim();
    if(required&&!curVal){
      curVal=variants[0]||"";
      input.value="";
      toast("«можно не указывать» = нет — выбран первый вариант из списка");
    }else{
      const r=resolveDropdownCustomValue(f, curVal);
      if(r.mode==="chip"){curVal=r.chip;input.value=""}
      else if(r.mode==="custom"){input.value=r.custom}
      else input.value="";
    }
    onChange(curVal);
    paintChips();
  });
  input.addEventListener("blur",()=>{
    if(required&&!String(curVal).trim()){
      curVal=variants[0]||"";
      input.value="";
      onChange(curVal);
      paintChips();
    }
    host.closest(".field")?.classList.remove("is-focus");
  });
  input.addEventListener("focus",()=>host.closest(".field")?.classList.add("is-focus"));
  wrap.appendChild(box);wrap.appendChild(custom);host.appendChild(wrap);paintChips();
}

function isEditedValue(path, val){
  const b=baseline();
  const parts=path.split(".");
  let cur=b;for(const p of parts){if(cur==null){cur=undefined;break}if(/^\d+$/.test(p))cur=cur[Number(p)];else cur=cur[p]}
  return String(cur??"")!==String(val??"");
}

function jsonStoreLeaf(f, sectionPrefix, legacyPrefix){
  const jt=String(f.json_target||"").trim();
  if(sectionPrefix&&jt.startsWith(sectionPrefix+"."))return jt.slice(sectionPrefix.length+1);
  const key=String(f.key||"");
  if(sectionPrefix&&key.startsWith(sectionPrefix+"."))return key.slice(sectionPrefix.length+1);
  if(legacyPrefix&&key.startsWith(legacyPrefix+"."))return key.slice(legacyPrefix.length+1);
  if(key.includes("."))return key.split(".").slice(1).join(".");
  return key;
}
function jsonPackLeaf(f, prefix){
  const jt=String(f.json_target||"").trim();
  if(prefix&&jt.startsWith(prefix+"."))return jt.slice(prefix.length+1);
  if(jt&&!jt.includes("."))return jt;
  return jsonStoreLeaf(f, prefix, prefix==="CONTEST_FEATURE"?"FEATURE":prefix==="REWARD_ADD_DATA"?"ADD":"");
}

