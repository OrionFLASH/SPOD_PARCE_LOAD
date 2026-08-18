/* game_fill_boot.js — привязка кнопок и старт */
"use strict";
function wire(){
  initTips();wireOutsideDate();
  setSidebarOpen(true);
  setFiltersOpen(true);
  remountContestListDateFilter();
  $("btn-sidebar-hide")?.addEventListener("click",()=>setSidebarOpen(false));
  $("btn-sidebar-show")?.addEventListener("click",()=>setSidebarOpen(true));
  $("btn-filters-hide")?.addEventListener("click",()=>setFiltersOpen(false));
  $("btn-filters-show")?.addEventListener("click",()=>setFiltersOpen(true));
  $("btn-add-contest").addEventListener("click",()=>addContest());
  $("btn-copy-contest")?.addEventListener("click",()=>openCopyContestModal());
  $("btn-select-mode")?.addEventListener("click",()=>setContestSelectMode(!contestSelectMode));
  $("btn-select-all")?.addEventListener("click",()=>selectVisibleContests());
  $("btn-select-none")?.addEventListener("click",()=>clearSelectedContests());
  $("btn-save-project").addEventListener("click",()=>saveProjectFile());
  $("btn-reset-catalog").addEventListener("click",()=>resetToCatalogDefaults());
  $("btn-export-all").addEventListener("click",()=>{if(!sessionReady){toast("Сначала создайте или откройте конкурс");return}exportAll()});
  document.querySelectorAll("[data-export]").forEach(btn=>btn.addEventListener("click",()=>{if(!sessionReady){toast("Сначала создайте или откройте конкурс");return}exportOne(btn.getAttribute("data-export"))}));
  $("import-project")?.addEventListener("change",e=>{
    const f=e.target.files&&e.target.files[0];
    onImportProjectFile(f);
    e.target.value="";
  });
  $("contest-search")?.addEventListener("input",e=>{
    contestListQuery=String(e.target.value||"");
    renderContestTabs();
  });
  document.querySelectorAll("[data-search-mode]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const m=btn.getAttribute("data-search-mode")||"contains";
      if(m!=="starts"&&m!=="contains"&&m!=="equals") return;
      contestListSearchMode=m;
      renderContestTabs();
    });
  });
  $("filter-tournaments")?.addEventListener("click",()=>{
    contestListShowTournament=!contestListShowTournament;
    renderContestTabs();
  });
  $("filter-rewards")?.addEventListener("click",()=>{
    contestListShowReward=!contestListShowReward;
    renderContestTabs();
  });
  $("filter-archive")?.addEventListener("click",()=>{
    contestListShowArchive=!contestListShowArchive;
    if(!contestListShowArchive&&activeArchiveId){activeArchiveId=null;render();return}
    renderContestTabs();
  });
  $("filter-env-prom")?.addEventListener("click",()=>{
    contestListShowProm=!contestListShowProm;
    renderContestTabs();
  });
  $("filter-env-test")?.addEventListener("click",()=>{
    contestListShowTest=!contestListShowTest;
    renderContestTabs();
  });
  document.querySelectorAll("[data-stand]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const t=btn.getAttribute("data-stand")||"";
      if(!t) return;
      if(contestListStandFilter.has(t)) contestListStandFilter.delete(t);
      else contestListStandFilter.add(t);
      renderContestTabs();
    });
  });
  document.querySelectorAll("[data-status]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const st=btn.getAttribute("data-status")||"";
      if(!st) return;
      if(contestListStatuses.has(st)) contestListStatuses.delete(st);
      else contestListStatuses.add(st);
      renderContestTabs();
    });
  });
  document.querySelectorAll("[data-bb]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const t=btn.getAttribute("data-bb")||"";
      if(!t) return;
      if(contestListBusinessBlocks.has(t)) contestListBusinessBlocks.delete(t);
      else contestListBusinessBlocks.add(t);
      renderContestTabs();
    });
  });
  $("filter-date-clear")?.addEventListener("click",()=>clearFilterGroup("date"));
  document.querySelectorAll(".filter-block__act[data-filter-act]").forEach(btn=>{
    btn.addEventListener("click",e=>{
      e.stopPropagation();
      const block=btn.closest("[data-filter-group]");
      const group=block&&block.getAttribute("data-filter-group");
      if(!group) return;
      const act=btn.getAttribute("data-filter-act");
      if(act==="on") enableFilterGroup(group);
      else if(act==="off") clearFilterGroup(group);
    });
  });
  $("filter-all-off")?.addEventListener("click",()=>clearAllListFilters());
  $("filter-all-on")?.addEventListener("click",()=>enableAllListFiltersExceptDate());
}
wire();boot();
