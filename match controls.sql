---------------------------------------------------------------------------------------------------
--match_controls.sql
--
--Dan Thayer
--
--Copyright 2017 Swansea University
--
--Licensed under the Apache License, Version 2.0 (the "License");
--you may not use this file except in compliance with the License.
--You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
--Unless required by applicable law or agreed to in writing, software
--distributed under the License is distributed on an "AS IS" BASIS,
--WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--See the License for the specific language governing permissions and
--limitations under the License.
--
---------------------------------------------------------------------------------------------------
--
--A procedure for creating a matched control group.  Matches on Week of birth (within a specified 
--number of days), gender, and optionally GP practice and area-based (LSOA code) measures.  
--It also optionally enforces follow-up time requirements in cases and controls.
--

--set to x schema for dev or 0 schema for release
set current schema sailx402v!
--set current schema sail0402v!

--A table that stores all potential case-control pairs, and is then gradually whittled down until
--the selected pairs are all that remain.
declare global temporary table session.temp_pairs (
    rand_row            float,
    alf_e               integer not null, 
    case_alf_e          integer not null,
    wob                 date,
    gender              integer,
    dod                 date,
    match_distance      integer,
    use_this_one        integer,
    used                integer,
    potential_matches   integer,
    matches             integer,
    available_from      timestamp
) with replace on commit preserve rows!

--A cut down version of the Welsh Demographic Service data (the base population for the SAIL 
--Databank).  This is done to increase performance, as well as to remove duplicates that can
--complicate queries.
declare global temporary table session.wds_sample (
    alf_e int,
    wob date,
    dod date,
    gndr_cd int,
    avail_from_dt timestamp
) with replace on commit preserve rows!

--Temporary staging table of cases and calculated variables to speed up performance.
declare global temporary table session.cases (
    alf_e           int,
    wob             date,
    match_date      date,
    dod             date,
    followup_start  date,
    followup_end    date,
    match_start     date,
    match_end       date,
    gndr_cd         int,
    prac_cd_e       int
) with replace on commit preserve rows!

--Working table of potential case-control pairs, which is gradually whittled down until the pairs
--that will be used are selected.
declare global temporary table session.temp_pairs (
    rand_row            float,
    alf_e               int not null, 
    case_alf_e          int not null,
    wob                 date,
    match_date          date,
    dod                 date,
    gender              int,
    prac_cd_e           int,
    match_distance      int,
    use_this_one        int,
    used                int,
    potential_matches   int,
    matches             int,
    available_from      timestamp
) with replace on commit preserve rows distribute by hash(alf_e)!


declare global temporary table session.valid_alfs (
    alf_e int not null
) with replace on commit preserve rows!

declare global temporary table session.match_wob (
    wob date,
    gndr_cd int
) with replace on commit preserve rows!

declare global temporary table session.gp_data like sailx320v.cleaned_gp_registrations_20151008
    with replace on commit preserve rows!
    
declare global temporary table session.welsh_res like sailx320v.welsh_residence
    with replace on commit preserve rows!

call fnc.drop_specific_proc_if_exists('match_controls')!
create procedure match_controls (
    input_table         varchar(100),   --Table of cases.  Must have a column ALF_E--individual
                                        --identifier within SAIL.
    census_date_column  varchar(100),   --Column defining census date within input_table
    output_table        varchar(100),   --The name of the output table (created for you)
    log_table           varchar(100),   --Log table for messages (created for you or appended).

    --Basic matching criteria
    num_controls        int,    --Number of controls to match per case
    max_match_distance  int,    --Max days' difference in WOB between cases and matched controls.
    
    --Additional optional matching criteria    
    match_practice  int default 0,              --Match on registration at same GP practice?
    lsoa_match_cols varchar(255) default '',    --A comma-delimited list of columns from the table
                                                --sailrefrv.lsoa_refr on which to match. These are
                                                --all area-based (LSOA) measures.
    lsoa_table      varchar(100) default 'sailx402v.lsoa',  --Table of LSOA follow-up data. Must
                                                            --have columns ALF_E, LSOA_CD,
                                                            --START_DATE,END_DATE
    --Requirements for followup time
    days_followup_before    int default 0,              --Follow-up to require before census date
    days_followup_after     int default 0,              --Follow-up to require after census date
    require_gp_data         int default 0,              --Require follow-up time in GP data?
    require_case_followup     int default 1,            --Exclude cases without follow-up time?
    pop_start_date          date default '0001-01-01',  --Overall start and end date of the study
    pop_end_date            date default '9999-12-31',

    --Tables that define the followup time in Wales and in SAIL GP data
    welsh_res_table varchar(100) default 'sailx402v.welsh_res',  --ALF_E,START_DATE,END_DATE
    clean_gp_table  varchar(100) default 'sailx402v.cleaned_gp', --ALF_E,PRAC_CD_E,START_DATE,END_DATE

    --Tables for exclusions
    exclude_alf_table       varchar(100) default '', --people to completely exclude
    exclude_control_table   varchar(100) default '', --people to exclude from the controls only

    match_ranking_method int default 1,  --1 = optimum, 2 = randomize (don't opt for distance), 3 = naively select closest matches
    --1=use a deterministic order for pairs that otherwise tie, 2=use rand()
    deterministic_order        int default 0,   
    --Export a full table of potential case-control pairs.  May be useful for debugging or using a 
    --custom matching algorithm. --1= output pair table and continue with matching; 
    --2=output pair table and stop.
    export_full_pair_table    int default 0 
)
specific sailx402v.match_controls
modifies sql data 
language sql
begin
    declare prev_total_matches          int default -1;
    declare total_matches               int default 0;
    declare num_rows                    int default 0;
    declare pass                        int default 1;
    declare lsoa_match_cols_cleaned     varchar(1000) default '';
    declare cs_lsoa_join_clause         varchar(400)  default '';
    declare ctrl_lsoa_join_clause       varchar(1000) default '';
    declare lsoa_cols_clause            varchar(1000) default '';
    declare lsoa_output_join_clause     varchar(1000) default '';
    declare gp_join_clause              varchar(200)  default '';
    declare lsoa_select_cols            varchar(200)  default '';
    declare case_match_ranking_clause   varchar(100)  default '';
    declare ctrl_match_ranking_clause   varchar(100)  default '';

    call fnc.log_start('match_controls',log_table);
    commit;

    --A table finding all distinct combinations of wob, gndr in the cases, used to cut down the
    --population to only those that will possibly be matches.
    declare global temporary table session.match_wob (
        wob date,
        gndr_cd int
    ) with replace on commit preserve rows;

    execute immediate 
        'insert into session.match_wob select distinct wob, gndr_cd from '||input_table;

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' combinations of WOB and gender in WDS.');

    declare global temporary table session.lsoa_cols (
        name varchar(255),
        type varchar(255)
    ) with replace on commit preserve rows;

    --If there are any LSOA-based matching variables, process them here.
    if lsoa_match_cols <> '' then
        call fnc.log_msg_commit('Processing LSOA match columns.');
    
        --Remove any spaces from the string
        set lsoa_match_cols_cleaned = replace(lsoa_match_cols,' ','');
        
        --Use a recursive query to split the comma-delimited list of columns into rows in a table.
        insert into session.lsoa_cols(name)
            with recurse_cols (colname,remaining_string) as (
                select 
                       --If there is a comma in the field, take the portion before the comma (the first value).
                       --Otherwise, take the whole field (there is only one value).
                       case when posstr(lsoa_match_cols_cleaned,',') > 0 then
                           left(lsoa_match_cols_cleaned,posstr(lsoa_match_cols_cleaned,',') - 1)
                           else lsoa_match_cols_cleaned
                       end as colname,
                       --If there was a comma, put everything after the comma into a new column called remaining_string,
                       --which will be used for future processing.
                       case when posstr(lsoa_match_cols_cleaned,',') > 0 then
                           substr(lsoa_match_cols_cleaned,posstr(lsoa_match_cols_cleaned,',') + 1)
                           else null
                       end as remaining_string
                   from sysibm.sysdummy1
               union all
                --Then union all to a query that is similar to above, but running on the subquery itself.
                --This will use up all the remaining values in the field, until remaining_string is null.
                select case when posstr(remaining_string,',') > 0 then
                           left(remaining_string,posstr(remaining_string,',') - 1)
                           else remaining_string
                       end as colname,
                       case when posstr(remaining_string,',') > 0 then
                           substr(remaining_string,posstr(remaining_string,',') + 1)
                           else null
                       end as remaining_string
                   from recurse_cols
                   where remaining_string is not null
            )
            --Convert column names to upper case for joining to the system table (syscat.columns)
            select distinct upper(colname) from recurse_cols;

            --Look in the system table to determine the type of each column.
            merge into session.lsoa_cols col
            using syscat.columns syscol 
            on  col.name = syscol.colname and
                tabschema = 'SAILREFRV' and
                tabname = 'LSOA_REFR'
            when matched then update set
                col.type = 
                    typename ||
                    case 
                        when typename like '%CHAR%' then '(' || LENGTH || ')'
                        when typename = 'DECIMAL' then '(' || LENGTH || ',' || SCALE || ')'
                        else ''
                    end;

            --Clause for joining to the LSOA information of the cases within dynamic SQL below.
            set cs_lsoa_join_clause = 
                ' join '||lsoa_table||' cs_lsoa on cs_lsoa.alf_e = cs.alf_e and '||
                   'cs.'||census_date_column||' between cs_lsoa.start_date and cs_lsoa.end_date '||
                'join sailrefrv.lsoa_refr cs_lsoa_refr on cs_lsoa.lsoa_cd = cs_lsoa_refr.lsoa_cd ';
            
            --Clause for joining to the LSOA information of the potential controls.
            set ctrl_lsoa_join_clause = 
                ' join '||lsoa_table||' ctrl_lsoa on ctrl_lsoa.alf_e = ctrl.alf_e and '||     
                'cs.match_date between ctrl_lsoa.start_date and ctrl_lsoa.end_date '||
                'join sailrefrv.lsoa_refr ctrl_lsoa_refr on ctrl_lsoa.lsoa_cd = ctrl_lsoa_refr.lsoa_cd '
                ;
               
            for col_add as select * from session.lsoa_cols order by name do
                --Finish the control LSOA join clause by requiring each column to match between
                --subjects and controls.
                set ctrl_lsoa_join_clause = ctrl_lsoa_join_clause || 
                    ' AND cs.'||name||' = ctrl_lsoa_refr.'||name;
                
                --Also create a clause for adding the LSOA columns when creating the output table.
                set lsoa_cols_clause = lsoa_cols_clause ||
                    ', '||name||' '||type;
                    
                --Yet another clause, for selecting the LSOA columns into the ouput table.
                set lsoa_select_cols = lsoa_select_cols || ', cs_lsoa_refr.'||name;
            end for;
            
            commit;
    end if;

    --Identify all the individuals that are not excluded and are close enough to potentially match.
    declare global temporary table session.valid_alfs (
        alf_e int not null
    ) with replace on commit preserve rows;
    create unique index session.va on session.valid_alfs (alf_e);
    commit;
    
    execute immediate '
        insert into session.valid_alfs
            select distinct alf_e from sailwdsdv.ar_pers person
                join session.match_wob match on
                    match.gndr_cd = person.gndr_cd and
                    match.wob between 
                        person.wob - '||max_match_distance||' days and 
                        person.wob + '||max_match_distance||' days
    ';

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' rows in WDS have a match to a case on WOB and gender.');

    execute immediate '
        declare global temporary table session.welsh_res like '||welsh_res_table||'
            with replace on commit preserve rows not logged
    ';
    create index session.res on session.welsh_res (alf_e);
    
    execute immediate '
        insert into session.welsh_res 
            select res.* from '||welsh_res_table||' res
                join session.valid_alfs alf on
                    res.alf_e = alf.alf_e and
                    days(end_date) - days(start_date) >= 
                    '||(days_followup_before + days_followup_after)||'
    ';

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' welsh residence rows for matching WDS rows.');

    execute immediate '
        declare global temporary table session.gp_data like '||clean_gp_table||'
            with replace on commit preserve rows not logged
    ';
    create index session.gp on session.gp_data (alf_e);

    if require_gp_data = 1 then 
    
        execute immediate '
            insert into session.gp_data 
                select gp.* from '||clean_gp_table||' gp
                    join session.valid_alfs alf on
                        gp.alf_e = alf.alf_e and
                        days(end_date) - days(start_date) >= 
                        '||(days_followup_before + days_followup_after)||'
        ';

        get diagnostics num_rows = row_count;
        call  fnc.log_msg_commit(num_rows || ' GP registration rows for matching WDS rows.');

        commit;
        truncate table session.valid_alfs immediate;

        insert into session.valid_alfs
            select distinct gp.alf_e 
                from session.gp_data gp
                join session.welsh_res res on
                    gp.alf_e = res.alf_e;
    else
        commit;
        truncate table session.valid_alfs immediate;

        insert into session.valid_alfs
            select distinct alf_e from session.welsh_res;
    end if;

    get diagnostics num_rows = row_count;
    call fnc.log_msg_commit(
        num_rows||' valid alfs who have at least one period of follow-up long enough.'
    );

    --If there is a table of alfs to exclude, delete them now.
    if exclude_alf_table <> '' then
        execute immediate '
            merge into session.valid_alfs valid
                using (select distinct alf_e from '||exclude_alf_table||') exclude
                on valid.alf_e = exclude.alf_e
            when matched then delete
        ';
    end if;

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' alfs excluded.');

    --Create a deduplicated version of the WDS person table to avoid multiple rows for one alf_e
    declare global temporary table session.wds_sample (
        alf_e int,
        wob date,
        dod date,
        gndr_cd int,
        avail_from_dt timestamp
    ) with replace on commit preserve rows;
    commit;

    call fnc.drop_index_if_exists('session.wd');
    call fnc.drop_index_if_exists('session.wd_wobgndr');
    create unique index session.wd on session.wds_sample (alf_e);
    create index session.wd_wobgndr on session.wds_sample(wob,gndr_cd);

    insert into session.wds_sample
        select  wds.alf_e,
                wds.wob,
                --Set DOD to 9999 for people still alive so we don't have to handle nulls when
                --joining. Will be set back to null later.
                coalesce(dod,'9999-12-31'),
                wds.gndr_cd,
                wds.avail_from_dt 
        from (
            --Use this query to only select the first occurrence if an ALF is duplicated.
            select count(*) over (partition by p.alf_e) as rown,p.* from sailwdsdv.ar_pers p
                join session.valid_alfs valid on
                    valid.alf_e = p.alf_e
        ) wds
        where rown=1;    

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' rows in WDS sample table.');

    --temporary table used for matching.
    declare global temporary table session.temp_pairs (
        rand_row            float,
        alf_e               integer not null, 
        case_alf_e          integer not null,
        wob                 date,
        match_date          date,
        dod                 date,
        gender              integer,
        prac_cd_e           integer,
        match_distance  integer,
        use_this_one        integer,
        used                integer,
        potential_matches   integer,
        matches             integer,
        available_from      timestamp
    ) with replace on commit preserve rows not logged distribute by hash(alf_e);

    --A temporary table for the cases is pre-computed, used to speed up the main join.
    execute immediate '
        declare global temporary table session.cases (
            alf_e          int,
            wob            date,
            match_date     date,
            dod            date,
            followup_start date,
            followup_end   date,
            match_start    date,
            match_end      date,
            gndr_cd        int,
            prac_cd_e      int
            '||lsoa_cols_clause||'
        ) with replace on commit preserve rows not logged
    ';

    call fnc.drop_index_if_exists('session.s');
    call fnc.drop_index_if_exists('session.s_wob_gndr');
    create unique index session.s on session.cases (alf_e,match_date);
    create index session.s_wob_gndr on session.cases (wob,gndr_cd);
    commit;

    execute immediate 
        'insert into session.cases 
            select  cs.alf_e,wob,
                    cs.'||census_date_column||',
                    dod,
                    cs.'||census_date_column||' - ('||days_followup_before||' days),
                    cs.'||census_date_column||' + ('||days_followup_after||'  days),
                    wob - ('||max_match_distance||' days),
                    wob + ('||max_match_distance||' days),
                    gndr_cd,
                    '||case when match_practice = 1 then 'prac_cd_e' else 'null' end|| 
                    lsoa_select_cols||
                ' from '||input_table||' cs '||
                case when require_case_followup = 1 and require_gp_data = 1 then
                    'join session.welsh_res cs_res on
                        cs.'||census_date_column||' between '''||pop_start_date||''' and '''||pop_end_date||''' and
                        cs_res.alf_e = cs.alf_e and
                        cs_res.start_date   <= cs.'||census_date_column||' - ('||days_followup_before||' days) and
                        cs_res.end_date >= min(cs.'||census_date_column||' + ('||days_followup_after||' days),coalesce(cs.dod,''9999-12-31''))
                    '
                else ''
                end|| 
                case when require_gp_data = 1 and require_case_followup = 1 then 
                    'join session.gp_data cs_gp on
                        cs_gp.alf_e = cs.alf_e and
                        cs_gp.start_date <= cs.'||census_date_column||' - ('||days_followup_before||' days) and
                        cs_gp.end_date >= min(cs.'||census_date_column||' + ('||days_followup_after||' days),coalesce(cs.dod,''9999-12-31''))
                    '
                    else ''
                end||
                cs_lsoa_join_clause;

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' cases will be matched.');

    --delete cases from potential control table.
    execute immediate '
        merge into session.wds_sample d
            using (select distinct alf_e from '||input_table||') s
            on s.alf_E = d.alf_e
            when matched then delete
    ';

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' cases deleted from potential control table.');
    
    create unique index session.tp on session.temp_pairs (alf_e,case_alf_e,match_date);
    commit;

    truncate table session.temp_pairs immediate;
    
    if require_gp_data=1 then
        set gp_join_clause = 
            ' join session.gp_data ctrl_gp on ctrl_gp.alf_e = ctrl.alf_e and '||
            ' ctrl_gp.start_date <= followup_start and ctrl_gp.end_date >= min(followup_end,ctrl.dod)'||
            case when match_practice=1 then ' and ctrl_gp.prac_cd_e = cs.prac_cd_e' else '' end;
    end if;
    
    if exclude_control_table <> '' then
        execute immediate '
            merge into session.wds_sample d
                using (select distinct alf_e from '||exclude_control_table||') s
                on s.alf_E = d.alf_e
                when matched then delete
        ';
    end if;
    
    execute immediate 
        'insert into session.temp_pairs 
               select   '||
                        case 
                            when deterministic_order = 0 then 'rand()'
                            else 'row_number() over (order by mod(ctrl.alf_e+cs.alf_e,100),ctrl.alf_e,cs.alf_e)'
                        end||'
                        ,ctrl.alf_e,cs.alf_e,ctrl.wob,match_date,
                       case when ctrl.dod = ''9999-12-31'' then null else ctrl.dod end,
                       ctrl.gndr_cd,cs.prac_cd_e,
                       abs(days(cs.wob)-days(ctrl.wob)),    --match precision
                    0,0,0,0,
                    current timestamp
                from session.cases cs
                join session.wds_sample  ctrl on
                    cs.gndr_cd = ctrl.gndr_cd and
                    ctrl.dod > cs.match_date and
                    ctrl.wob between match_start and match_end
                join session.welsh_res ctrl_res on
                    ctrl_res.alf_e = ctrl.alf_e and
                    ctrl_res.start_date <= followup_start and
                    ctrl_res.end_date >= min(followup_end,ctrl.dod)
                '||gp_join_clause||ctrl_lsoa_join_clause;

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' potential match pairs identified.');

    --export the entire table of potential pairs if requested.
    if export_full_pair_table>0 then
        execute immediate 'create table '||output_table||'_all_potential_pairs like session.temp_pairs';
        execute immediate 'insert into '||output_table||'_all_potential_pairs select * from session.temp_pairs';

        call fnc.log_msg_commit(
            'exported full table of potential pairs into '
            ||output_table||'_all_potential_pairs'
        );
        
        --export the entire table of potential pairs and then quit if the value is 2.
        if export_full_pair_table=2 then
            call fnc.log_finish();
            return;
        end if;
    end if;


    
    --Count the number of potential controls for each case.
    update (
            select  potential_matches,
                    count(alf_e) over (partition by case_alf_e,match_date) as potential
                from session.temp_pairs
        )
        set potential_matches = potential;


    if match_ranking_method = 1 then
        set case_match_ranking_clause = 'matches,potential_matches,rand_row';
        set ctrl_match_ranking_clause = 'match_distance,rand_row';
    elseif match_ranking_method = 2 then
        set case_match_ranking_clause = 'rand_row';
        set ctrl_match_ranking_clause = 'rand_row';
    elseif match_ranking_method = 3 then
        set case_match_ranking_clause = 'match_distance,rand_row';
        set ctrl_match_ranking_clause = 'match_distance,rand_row';
    end if;

    --Select matches using an iterative process.  Stop when a loop iteration selected no new matches.
    while prev_total_matches <> total_matches do
        set prev_total_matches = total_matches;
        
        if match_ranking_method in (1,3) then
            --preselect matched pairs where the control could only match one case and is one of the top
            --matches (by distance) for that case.  Do not do this if random selection is enabled.
            update (
                select * from (
                    select  match_count.*,
                            row_number() over (
                                partition by case_alf_e,match_date order by match_distance,matched_cases
                            ) match_rank
                        from (
                            select  case_alf_e,
                                    match_distance,
                                    match_date,
                                    matches,
                                    use_this_one,
                                    used,
                                    count(case_alf_e||match_date) over (partition by alf_e) matched_cases
                                from session.temp_pairs
                                where used = 0
                        ) match_count
                )
                where matched_cases = 1 and match_rank <= num_controls-matches
            )
            set use_this_one = 1,
                used = 1;
                
                

            call fnc.log_msg(
                'Mean square difference between case and control WOB: '||
                (select avg(decimal(match_distance,31,4)) from session.temp_pairs where use_this_one=1) ||
                ' days.'
            );

            --Count the total number of matched pairs that have been selected.
            set total_matches = (select count(1) from session.temp_pairs where use_this_one = 1);

            call  fnc.log_msg_commit('(Pass '||pass||') '||total_matches|| 
                ' total matches after selecting pairs where the control only matched one case.'
            );
    
            --Update the count of matches and potential matches related to the case for each row.
            merge into session.temp_pairs tp
            using (
                select case_alf_e,match_date,sum(use_this_one) match_count
                    from session.temp_pairs
                    group by case_alf_e,match_date
            ) counts
            on  tp.case_alf_e = counts.case_alf_e and
                tp.match_date = counts.match_date
            when matched then update set
                matches = match_count,
                potential_matches = potential_matches - (match_count - matches);
            
            call  fnc.log_msg_commit('(Pass '||pass||') update match count.');
 
            
            --delete potential pairs that now cannot be used, or are not needed.
            delete from session.temp_pairs 
                where 
                    (matches = num_controls and use_this_one = 0);
    
            call  fnc.log_msg_commit('(Pass '||pass||') delete unneeded pairs.');

        end if;
        
        execute immediate '
            update (
                select * from ( 
                    select best_match.*, 
                            row_number() over (
                                    partition by case_alf_e,match_date 
                                    order by '||ctrl_match_ranking_clause||'
                                ) as top_control
                        from ( select * from (
                        select  alf_e, 
                                case_alf_e,
                                match_date,
                                match_distance,
                                rand_row,
                                used,
                                use_this_one,
                                potential_matches,
                                row_number() over (
                                    partition by alf_e order by '||case_match_ranking_clause||'
                                ) as top_case
                            from session.temp_pairs
                            where used = 0 and matches < '||num_controls||'
                    ) 
                    where top_case = 1) best_match
                )
                --select only rows that are both the top ranked case and the top ranked control.
                where top_control = 1 
            ) 
            set used = 1,
            use_this_one = 1
        ';

            call fnc.log_msg(
                'Mean square difference between case and control WOB: '||
                (select avg(decimal(match_distance,31,4)) from session.temp_pairs where use_this_one=1) ||
                ' days.'
            );

        --Count the total number of matched pairs that have been selected.
        set total_matches = (select count(1) from session.temp_pairs where use_this_one = 1);
        
        call  fnc.log_msg_commit('(Pass '||pass||') '||total_matches||
            ' total matches after selecting top ranked pairs.'
        );

 
        --If a control has been used in a selected pair, mark all potential pairs containing that
        --control as used.
        update (
                select  used,
                        max(used) over (partition by alf_e) as ever_used
                    from session.temp_pairs
            )
            set used = ever_used;

        call  fnc.log_msg_commit('(Pass '||pass||') mark controls that have been selected as used in all rows.');

        
        --Update the count of matches and potential matches related to the case for each row.
        merge into session.temp_pairs tp
        using (
            select case_alf_e,match_date,sum(use_this_one) match_count
                from session.temp_pairs
                group by case_alf_e,match_date
        ) counts
        on  tp.case_alf_e = counts.case_alf_e and
            tp.match_date = counts.match_date
        when matched then update set
            matches = match_count,
            potential_matches = potential_matches - (match_count - matches);

        call  fnc.log_msg_commit('(Pass '||pass||') update match counts.');

        
        --delete potential pairs that now cannot be used, or are not needed.
        delete from session.temp_pairs 
            where 
                (used = 1 and use_this_one    = 0) or
                (matches = num_controls and use_this_one = 0);
            
        call  fnc.log_msg_commit('(Pass '||pass||') delete unneeded pairs.');
            
        set pass = pass + 1;

    end while;

    --Delete all unused pairs.
    delete from session.temp_pairs where use_this_one = 0;

    get diagnostics num_rows = row_count;
    call  fnc.log_msg_commit(num_rows || ' unused match pairs deleted.');

    --Create the output table
    execute immediate '
        create table '||output_table||' (
            alf_e           int not null,
            is_case         int,
            matched_group   int,
            case_alf_e      int,
            match_date      date,
            wob             date,
            gndr_cd         int,
            dod             date,
            prac_cd_e       int,
            welsh_res_start date,
            welsh_res_end   date,
            gp_data_start   date,
            gp_data_end     date
            '||lsoa_cols_clause||'
        )';
    commit;

    --Put the cases into the output table.
    execute immediate '
    insert into '||output_table||' (
            alf_e, is_case, matched_group, case_alf_e, match_date, wob, gndr_cd,
            dod, prac_cd_e, welsh_res_start, welsh_res_end, gp_data_start, gp_data_end
        )
        select  cs.alf_e,1,
                row_number() over (),
                cs.alf_e,
                cs.'||census_date_column||',
                wob,gndr_cd,dod,cs_gp.prac_cd_e,
                cs_res.start_date,cs_res.end_date,
                cs_gp.start_date,cs_gp.end_date
            from '||input_table||' cs
            left join session.welsh_res cs_res on
                   cs.'||census_date_column||' between '''||pop_start_date||''' and '''||pop_end_date||''' and
                cs_res.alf_e = cs.alf_e and
                cs_res.start_date <= cs.'||census_date_column||' - (('||days_followup_before||') days) and
                cs_res.end_date >= min(cs.'||census_date_column||' + (('||days_followup_after||') days),coalesce(cs.dod,''9999-12-31''))
            left join session.gp_data cs_gp on
                cs_gp.alf_e = cs.alf_e and
                cs_gp.start_date <= cs.'||census_date_column||' - (('||days_followup_before||') days) and
                cs_gp.end_date >= min(cs.'||census_date_column||' + (('||days_followup_after||') days),coalesce(cs.dod,''9999-12-31''))
            where 
                '||require_case_followup||' = 0 or (
                    cs_res.alf_E is not null and
                    ('||require_gp_data||' = 0  or cs_gp.alf_e is not null)
                )
    ';
    
    --Put the selected controls into the specified output table.
    execute immediate '
        insert into '||output_table||' (
                alf_e, is_case, matched_group, case_alf_e, match_date, wob, gndr_cd,
                dod, prac_cd_e, welsh_res_start, welsh_res_end, gp_data_start, gp_data_end
            )
            select    control.alf_e,
                    0,
                    cs.matched_group,
                    cs.case_alf_e,
                    cs.match_date,
                    control.wob,
                    control.gender,
                    case when wds.dod = ''9999-12-31'' then null else wds.dod end,
                    control.prac_cd_e,
                    control_res.start_date,
                    control_res.end_date,
                    control_gp.start_date,
                    control_gp.end_date
                from session.temp_pairs control
                join '||output_table||' cs on
                    cs.alf_e = control.case_alf_e and
                    cs.match_date = control.match_date
                join session.welsh_res control_res on
                    control_res.alf_e = control.alf_e and
                    cs.match_date between control_res.start_date and control_res.end_date
                join session.wds_sample wds on
                    wds.alf_e = control.alf_e
                left join session.gp_data control_gp on
                    control_gp.alf_e = control.alf_e and
                    cs.match_date between control_gp.start_date and control_gp.end_date
                where '||require_gp_data||' = 0 or control_gp.alf_e is not null
    ';

    --Add any extra lsoa-based variables that were matched on into the output table.
    for lsoa_val as select * from session.lsoa_cols order by name do
        execute immediate 'merge into '||output_table||' o
            using (
                select alf_e,start_date,end_date,lsoa_refr.'||name||' from '||lsoa_table||' lsoa 
                    join sailrefrv.lsoa_refr lsoa_refr on lsoa_refr.lsoa_cd = lsoa.lsoa_cd
            ) l
            on o.alf_E = l.alf_e and
                match_date between start_date and end_date
            when matched then update set
                o.'||name||' = l.'||name;
                
        call fnc.log_msg('Added column '||name||' to output.');
    end for;

    --write some log messages about the match performance.
    execute immediate '
        call fnc.log_msg(
            ''Mean square difference between case and control WOB: ''||
            (select decimal(avg(sqrt(power(days(cs.wob)-days(ctrl.wob),2))),31,4)
                from (select * from '||output_table||' where is_case=0) ctrl
                join (select * from '||output_table||' where is_case=1) cs on
                    cs.alf_e = ctrl.case_alf_e) ||
            '' days.''
        )
    ';

    execute immediate '
        call fnc.log_msg(
            ''Matching rate: ''||
            (((select count(*) from '||output_table||' where is_case=0)+0.0000) / 
            (select count(*) from '||output_table||' where is_case=1) / '||num_controls||' * 100)||
            ''%''
        )
    ';

    call fnc.log_finish();
end!
