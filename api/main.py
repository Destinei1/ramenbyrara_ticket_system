"""
=============================================================
 RAMEN BY RARA — FastAPI Backend
 Stack: FastAPI + Upstash Redis + Supabase Postgres
=============================================================
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
from datetime import datetime
from upstash_redis import Redis
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")  # Load .env file for local development

# ================================================================
#  CLIENTS — reads from .env file locally,
#            reads from Vercel Environment Variables in production
#
#  If you see an error here, check that your .env file has:
#    SUPABASE_URL, SUPABASE_SERVICE_KEY,
#    UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
# ================================================================
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],        # → from .env
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"]     # → from .env
)

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],                      # → from .env
    os.environ["SUPABASE_SERVICE_KEY"]               # → from .env
)

# ── APP ─────────────────────────────────────────────────────
app = FastAPI(title="Ramen by RaRa API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    # ================================================================
    #  actual Vercel URL
    # ================================================================
    allow_origins=["https://ramenbyrara-ticket-system.vercel.app/"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# ── CONSTANTS ───────────────────────────────────────────────
LOCK_TTL_SECONDS = 600       # 10 minutes — mirrors Redis TTL
SEATS = list(range(1, 9))    # seats 1–8


# ── HELPERS ─────────────────────────────────────────────────
def lock_key(seat_id: int, dining_date: str) -> str:
    """
    Redis key format: seat:ra:{seat_id}:{dining_date}
    Example:          seat:ra:3:2025-09-15
    """
    return f"seat:ra:{seat_id}:{dining_date}"


def log_event(event_type: str, payload: dict,
              seat_id=None, guest_id=None,
              reservation_id=None, waitlist_id=None):
    """
    Writes an immutable event to Supabase events table.
    This table syncs to Snowflake for analytics.
    Never crashes the main flow — errors are swallowed safely.
    """
    try:
        supabase.table("events").insert({
            "event_type":     event_type,
            "seat_id":        seat_id,
            "guest_id":       str(guest_id) if guest_id else None,
            "reservation_id": str(reservation_id) if reservation_id else None,
            "waitlist_id":    str(waitlist_id) if waitlist_id else None,
            "payload":        payload,
        }).execute()
    except Exception:
        pass


def get_or_create_guest(full_name: str, email: Optional[str],
                         phone: Optional[str]) -> dict:
    """
    Deduplicates guests by email first, then phone.
    Creates a new guest row if neither match.
    """
    if email:
        res = supabase.table("guests").select("*").eq("email", email).execute()
        if res.data:
            return res.data[0]
    if phone:
        res = supabase.table("guests").select("*").eq("phone", phone).execute()
        if res.data:
            return res.data[0]
    res = supabase.table("guests").insert({
        "full_name": full_name,
        "email":     email or None,
        "phone":     phone or None,
    }).execute()
    return res.data[0]


# ── REQUEST MODELS ───────────────────────────────────────────
class LockRequest(BaseModel):
    seat_id:     int
    dining_date: str    # format: YYYY-MM-DD
    session_id:  str    # browser UUID — identifies lock owner


class ReserveRequest(BaseModel):
    seat_id:     int
    dining_date: str
    session_id:  str
    full_name:   str
    email:       Optional[str] = None
    phone:       Optional[str] = None
    party_size:  int = 1


class WaitlistRequest(BaseModel):
    dining_date: str
    full_name:   str
    email:       Optional[str] = None
    phone:       Optional[str] = None
    party_size:  int = 1


# ════════════════════════════════════════════════════════════
#  ROUTES
# ════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"status": "RAMEN BY RARA API ONLINE 🍜"}


@app.get("/health")
def health():
    """Frontend pings this on load to check if API is alive."""
    return {"status": "ok", "redis": "upstash", "db": "supabase"}


# ── GET SEAT AVAILABILITY ────────────────────────────────────
@app.get("/seats/{dining_date}")
def get_seats(dining_date: str):
    """
    Returns all 8 seats with live status for a given date.
    Logic:
      1. Query Supabase for confirmed bookings on this date
      2. Check Redis for any active locks
      3. Everything else = available
    """
    confirmed_res = supabase.table("reservations") \
        .select("seat_id, reservation_id, guest_id, party_size") \
        .eq("dining_date", dining_date) \
        .eq("status", "confirmed") \
        .execute()

    confirmed_seats = {r["seat_id"]: r for r in confirmed_res.data}
    seat_statuses = []

    for seat_id in SEATS:
        key     = lock_key(seat_id, dining_date)
        lock_val = redis.get(key)
        ttl     = redis.ttl(key) if lock_val else -1

        if seat_id in confirmed_seats:
            status = "confirmed"
        elif lock_val:
            status = "locked"
        else:
            status = "available"

        seat_statuses.append({
            "seat_id":     seat_id,
            "status":      status,
            "lock_ttl":    ttl if status == "locked" else None,
            "reservation": confirmed_seats.get(seat_id),
        })

    return {"date": dining_date, "seats": seat_statuses}


# ── LOCK A SEAT ──────────────────────────────────────────────
@app.post("/lock")
def lock_seat(req: LockRequest):
    """
    Atomically locks a seat using Redis SET NX EX.

    Redis command: SET seat:ra:{seat_id}:{date} {session_id} NX EX 600
      NX  = Only sets if key does NOT already exist (mutual exclusion)
      EX  = Auto-expires after 600 seconds (prevents deadlocks)

    Returns 409 if seat is already locked or confirmed by someone else.
    """
    if req.seat_id not in SEATS:
        raise HTTPException(400, f"Invalid seat: {req.seat_id}")

    # Check Supabase — already confirmed?
    confirmed = supabase.table("reservations") \
        .select("reservation_id") \
        .eq("seat_id", req.seat_id) \
        .eq("dining_date", req.dining_date) \
        .eq("status", "confirmed") \
        .execute()

    if confirmed.data:
        raise HTTPException(409, "SEAT_TAKEN: Already confirmed by another guest")

    # Atomic Redis lock — SET NX EX
    key      = lock_key(req.seat_id, req.dining_date)
    acquired = redis.set(key, req.session_id, nx=True, ex=LOCK_TTL_SECONDS)

    if not acquired:
        current = redis.get(key)
        if current == req.session_id:
            return {"locked": True, "seat_id": req.seat_id,
                    "ttl": redis.ttl(key), "message": "Already locked by you"}
        raise HTTPException(409, "SEAT_LOCKED: Seat is held by another user")

    log_event("seat_locked", {
        "seat_id": req.seat_id,
        "dining_date": req.dining_date,
        "redis_key": key,
        "ttl": LOCK_TTL_SECONDS
    }, seat_id=req.seat_id)

    return {
        "locked":    True,
        "seat_id":   req.seat_id,
        "ttl":       LOCK_TTL_SECONDS,
        "redis_key": key,
        "message":   f"SET {key} NX EX {LOCK_TTL_SECONDS} → OK"
    }


# ── CONFIRM RESERVATION ──────────────────────────────────────
@app.post("/reserve")
def confirm_reservation(req: ReserveRequest):
    """
    Confirms a reservation:
      1. Verifies the session owns the Redis lock
      2. Upserts guest into Supabase
      3. Writes confirmed reservation row
      4. Deletes Redis lock (Supabase is now source of truth)
    """
    if req.party_size >= 4:
        raise HTTPException(400, "PARTY_SIZE: Maximum party size is 3")
    if not req.email and not req.phone:
        raise HTTPException(400, "CONTACT_REQUIRED: Provide email or phone")

    # Verify Redis lock ownership
    key        = lock_key(req.seat_id, req.dining_date)
    lock_owner = redis.get(key)

    if not lock_owner:
        raise HTTPException(409, "LOCK_EXPIRED: Lock expired — please re-select seat")
    if lock_owner != req.session_id:
        raise HTTPException(403, "LOCK_STOLEN: You don't own this lock")

    # Upsert guest
    try:
        guest = get_or_create_guest(req.full_name, req.email, req.phone)
    except Exception as e:
        raise HTTPException(400, f"GUEST_ERROR: {str(e)}")

    # Write to Supabase
    try:
        res = supabase.table("reservations").insert({
            "guest_id":       guest["guest_id"],
            "seat_id":        req.seat_id,
            "dining_date":    req.dining_date,
            "party_size":     req.party_size,
            "status":         "confirmed",
            "redis_lock_key": key,
            "confirmed_at":   datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        err = str(e)
        if "BLACKOUT" in err:
            raise HTTPException(409, "BLACKOUT: Already have a reservation within 4 weeks")
        if "no_double_booking" in err:
            raise HTTPException(409, "DOUBLE_BOOK: Seat already taken for this date")
        raise HTTPException(500, f"DB_ERROR: {err}")

    reservation = res.data[0]

    # Delete Redis lock — Supabase is now source of truth
    redis.delete(key)

    log_event("booking_confirmed", {
        "seat_id":    req.seat_id,
        "dining_date": req.dining_date,
        "party_size": req.party_size,
        "guest_name": req.full_name
    }, seat_id=req.seat_id,
       guest_id=guest["guest_id"],
       reservation_id=reservation["reservation_id"])

    return {
        "confirmed":      True,
        "reservation_id": reservation["reservation_id"],
        "seat_id":        req.seat_id,
        "dining_date":    req.dining_date,
        "guest_name":     req.full_name,
        "party_size":     req.party_size,
        "message":        f"🍜 Confirmed! See you at the counter, {req.full_name}!"
    }


# ── JOIN WAITLIST ────────────────────────────────────────────
@app.post("/waitlist")
def join_waitlist(req: WaitlistRequest):
    """Adds a guest to the waitlist queue for a given date."""
    if req.party_size >= 4:
        raise HTTPException(400, "PARTY_SIZE: Maximum party size is 3")
    if not req.email and not req.phone:
        raise HTTPException(400, "CONTACT_REQUIRED: Provide email or phone")

    try:
        guest = get_or_create_guest(req.full_name, req.email, req.phone)
    except Exception as e:
        raise HTTPException(400, f"GUEST_ERROR: {str(e)}")

    queue_res = supabase.table("waitlist") \
        .select("waitlist_id") \
        .eq("requested_date", req.dining_date) \
        .eq("status", "waiting") \
        .execute()
    position = len(queue_res.data) + 1

    try:
        res = supabase.table("waitlist").insert({
            "guest_id":       guest["guest_id"],
            "requested_date": req.dining_date,
            "party_size":     req.party_size,
            "queue_position": position,
            "status":         "waiting",
        }).execute()
    except Exception as e:
        err = str(e)
        if "WAITLIST_BLOCKED" in err:
            raise HTTPException(409, "WAITLIST_BLOCKED: Cancel your existing reservation first")
        if "one_waitlist_per_guest_date" in err:
            raise HTTPException(409, "ALREADY_WAITING: Already on waitlist for this date")
        raise HTTPException(500, f"DB_ERROR: {err}")

    waitlist_entry = res.data[0]

    log_event("waitlist_joined", {
        "dining_date": req.dining_date,
        "position":    position,
        "guest_name":  req.full_name,
        "party_size":  req.party_size
    }, guest_id=guest["guest_id"],
       waitlist_id=waitlist_entry["waitlist_id"])

    return {
        "joined":      True,
        "waitlist_id": waitlist_entry["waitlist_id"],
        "position":    position,
        "dining_date": req.dining_date,
        "message":     f"Added to waitlist at position #{position}"
    }


# ── CANCEL RESERVATION ───────────────────────────────────────
@app.post("/cancel/{reservation_id}")
def cancel_reservation(reservation_id: str):
    """
    Cancels a reservation and checks the waitlist.
    Returns the next waiting guest so the frontend can show the cat popup.
    """
    res = supabase.table("reservations") \
        .select("*, guests(full_name, email, phone)") \
        .eq("reservation_id", reservation_id) \
        .eq("status", "confirmed") \
        .execute()

    if not res.data:
        raise HTTPException(404, "RESERVATION_NOT_FOUND")

    reservation = res.data[0]

    supabase.table("reservations") \
        .update({"status": "cancelled",
                 "cancelled_at": datetime.utcnow().isoformat()}) \
        .eq("reservation_id", reservation_id) \
        .execute()

    # Clear Redis lock if still present
    redis.delete(lock_key(reservation["seat_id"], reservation["dining_date"]))

    log_event("booking_cancelled", {
        "seat_id":    reservation["seat_id"],
        "dining_date": reservation["dining_date"],
    }, seat_id=reservation["seat_id"],
       guest_id=reservation["guest_id"],
       reservation_id=reservation_id)

    # Check waitlist for next guest
    waitlist_res = supabase.table("waitlist") \
        .select("*, guests(full_name, email, phone)") \
        .eq("requested_date", reservation["dining_date"]) \
        .eq("status", "waiting") \
        .order("queue_position") \
        .limit(1) \
        .execute()

    next_guest = None
    if waitlist_res.data:
        next_entry = waitlist_res.data[0]
        supabase.table("waitlist") \
            .update({"status": "notified",
                     "notified_at": datetime.utcnow().isoformat()}) \
            .eq("waitlist_id", next_entry["waitlist_id"]) \
            .execute()
        next_guest = {
            "name":        next_entry["guests"]["full_name"],
            "email":       next_entry["guests"]["email"],
            "phone":       next_entry["guests"]["phone"],
            "waitlist_id": next_entry["waitlist_id"],
        }
        log_event("waitlist_notified", {
            "seat_id":        reservation["seat_id"],
            "dining_date":    reservation["dining_date"],
            "notified_guest": next_guest["name"]
        }, seat_id=reservation["seat_id"],
           waitlist_id=next_entry["waitlist_id"])

    return {
        "cancelled":        True,
        "reservation_id":   reservation_id,
        "seat_id":          reservation["seat_id"],
        "dining_date":      reservation["dining_date"],
        "next_on_waitlist": next_guest,
        "message": "Reservation cancelled" + (
            f" · Notifying {next_guest['name']}" if next_guest else " · No waitlist"
        )
    }


# ── GET WAITLIST ─────────────────────────────────────────────
@app.get("/waitlist/{dining_date}")
def get_waitlist(dining_date: str):
    """Returns current waitlist queue for a given date."""
    res = supabase.table("waitlist") \
        .select("*, guests(full_name, email, phone)") \
        .eq("requested_date", dining_date) \
        .eq("status", "waiting") \
        .order("queue_position") \
        .execute()

    return {
        "date":     dining_date,
        "count":    len(res.data),
        "waitlist": [{
            "position":   w["queue_position"],
            "name":       w["guests"]["full_name"],
            "party_size": w["party_size"],
            "joined_at":  w["joined_at"],
        } for w in res.data]
    }
