-- Run this once in the Supabase SQL Editor for the project used by the website.
-- The website should create one fresh code per media link. The code is a
-- bearer credential: anyone who has it can use that link until it expires.

create table if not exists public.media_access_codes (
  code text primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  active boolean not null default true,
  expires_at timestamptz,
  download_claimed boolean not null default false,
  stream_claimed boolean not null default false,
  created_at timestamptz not null default now()
);

create table if not exists public.media_usage_daily (
  user_id uuid primary key references auth.users(id) on delete cascade,
  usage_date date not null default current_date,
  downloads integer not null default 0 check (downloads >= 0),
  streams integer not null default 0 check (streams >= 0),
  updated_at timestamptz not null default now()
);

create index if not exists media_access_codes_user_id_idx
  on public.media_access_codes(user_id);

alter table public.media_access_codes enable row level security;
alter table public.media_usage_daily enable row level security;

grant select, insert on public.media_access_codes to authenticated;

drop policy if exists "Users can view their own media access codes"
  on public.media_access_codes;
create policy "Users can view their own media access codes"
  on public.media_access_codes for select
  to authenticated
  using (auth.uid() = user_id);

drop policy if exists "Users can create their own media access codes"
  on public.media_access_codes;
create policy "Users can create their own media access codes"
  on public.media_access_codes for insert
  to authenticated
  with check (auth.uid() = user_id);

create or replace function public.resolve_media_access_code(p_code text)
returns jsonb
language sql
security definer
set search_path = public
as $$
  select coalesce(
    (
      select jsonb_build_object(
        'valid', true,
        'user_id', user_id::text
      )
      from public.media_access_codes
      where code = trim(p_code)
        and active = true
        and (expires_at is null or expires_at > now())
      limit 1
    ),
    jsonb_build_object('valid', false)
  );
$$;

create or replace function public.claim_media_access(
  p_code text,
  p_action text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_code text := trim(p_code);
  v_user_id uuid;
  v_download_claimed boolean;
  v_stream_claimed boolean;
  v_downloads integer := 0;
  v_streams integer := 0;
  v_rows integer := 0;
begin
  if p_action not in ('download', 'stream') then
    return jsonb_build_object('allowed', false, 'reason', 'invalid_action');
  end if;

  -- Lock the code row first. This makes all ranged requests for one link
  -- idempotent and prevents two simultaneous requests from claiming it twice.
  select user_id, download_claimed, stream_claimed
    into v_user_id, v_download_claimed, v_stream_claimed
    from public.media_access_codes
   where code = v_code
     and active = true
     and (expires_at is null or expires_at > now())
   for update;

  if v_user_id is null then
    return jsonb_build_object('allowed', false, 'reason', 'invalid_code');
  end if;

  if (p_action = 'download' and v_download_claimed)
     or (p_action = 'stream' and v_stream_claimed) then
    return jsonb_build_object(
      'allowed', true,
      'already_claimed', true,
      'user_id', v_user_id::text
    );
  end if;

  -- Keep exactly one usage row per user. A new calendar day resets the same
  -- row instead of creating unbounded daily history.
  select downloads, streams
    into v_downloads, v_streams
    from public.media_usage_daily
   where user_id = v_user_id
   for update;

  if not found then
    insert into public.media_usage_daily (user_id, usage_date)
    values (v_user_id, current_date)
    on conflict (user_id) do nothing;
    get diagnostics v_rows = row_count;
    if v_rows = 0 then
      select downloads, streams
        into v_downloads, v_streams
        from public.media_usage_daily
       where user_id = v_user_id
       for update;
    end if;
  else
    -- The row is locked above; reset it in place when the date changes.
    if (select usage_date from public.media_usage_daily where user_id = v_user_id) <> current_date then
      update public.media_usage_daily
         set usage_date = current_date,
             downloads = 0,
             streams = 0,
             updated_at = now()
       where user_id = v_user_id;
      v_downloads := 0;
      v_streams := 0;
    end if;
  end if;

  if (p_action = 'download' and v_downloads >= 5)
     or (p_action = 'stream' and v_streams >= 5) then
    return jsonb_build_object(
      'allowed', false,
      'reason', 'daily_limit',
      'user_id', v_user_id::text
    );
  end if;

  update public.media_usage_daily
     set downloads = downloads + case when p_action = 'download' then 1 else 0 end,
         streams = streams + case when p_action = 'stream' then 1 else 0 end,
         updated_at = now()
   where user_id = v_user_id;

  update public.media_access_codes
     set download_claimed = download_claimed or p_action = 'download',
         stream_claimed = stream_claimed or p_action = 'stream'
   where code = v_code;

  return jsonb_build_object(
    'allowed', true,
    'already_claimed', false,
    'user_id', v_user_id::text,
    'downloads_remaining', greatest(0, 5 - v_downloads - case when p_action = 'download' then 1 else 0 end),
    'streams_remaining', greatest(0, 5 - v_streams - case when p_action = 'stream' then 1 else 0 end)
  );
end;
$$;

revoke all on function public.resolve_media_access_code(text) from public;
revoke all on function public.claim_media_access(text, text) from public;
grant execute on function public.resolve_media_access_code(text) to anon, authenticated;
grant execute on function public.claim_media_access(text, text) to anon, authenticated;
