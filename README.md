Finding all Load Plans in Oracle Data Integrator for PLSQL 

Change "ODI_REPO_NAME" with your repo name.

---

If  SYS.utl_mail package exist  and  loadplan.sql insert at db (for ex. DWH.LOADPLAN)  then you can use this mail procedure.

You can use after the compile procedure and run this block:
BEGIN DWH.PRC_LOADPLAN; END;

---
