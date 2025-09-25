import discord
from discord.utils import utcnow
import datetime
import pytz
import os
from dotenv import load_dotenv
import asyncio

################################################################################################
######################## Discord Event Sequence ############################################
################################################################################################
eastern = pytz.timezone('US/Eastern')
# Load environment variables from .env file
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Initiate Timezone (this line is redundant if already defined above)
# eastern = pytz.timezone('US/Eastern')

# Define intents (minimal for scheduled events and guild access)
intents = discord.Intents.default()
intents.guilds = True
intents.guild_scheduled_events = True # Crucial for scheduled events

# Create a discord.Client instance
client = discord.Client(intents=intents)

# Flag to indicate if the bot is ready
bot_ready_event = asyncio.Event()

# --- Core Function to Update/Create Discord Event ---
async def update_or_create_discord_event(event_name: str, event_description: str, event_start_time: datetime.datetime,
    event_end_time: datetime.datetime,  event_location: str):

    await bot_ready_event.wait()
    print("Bot confirmed ready before event operation.")

    guild = client.get_guild(GUILD_ID)
    # REMOVED: eastern.localize() calls as datetimes are already localized
    # event_start_time = eastern.localize(event_start_time)
    # event_end_time = eastern.localize(event_end_time)

    external_event_type = discord.EntityType.external
    scheduled_events = await guild.fetch_scheduled_events()
    found_event = None

    current_time = eastern.localize(datetime.datetime.now())
    for event in scheduled_events:
        # Check if event name matches and start times are very close
        # (event.start_time - event_start_time).total_seconds() gives difference in seconds
        # Using abs() to handle cases where times might be slightly off due to float precision or API conversions
        if event.name == event_name and abs((event.start_time - event_start_time).total_seconds()) < 60:
            found_event = event
            break
    try:
        if found_event:
            print(f'Editing Existing Event: {event_name} (ID: {found_event.id})!')
            # Compare already localized times
            if event_start_time > current_time:
                await found_event.edit(
                    name=event_name,
                    description=event_description,
                    location=event_location,
                    start_time=event_start_time,
                    end_time=event_end_time,
                    privacy_level=discord.PrivacyLevel.guild_only)
                print(f'Successfully updated event: {event_name}')
                return True
            print(f'Event in the past!')
            return False
        else:
            print(f'Creating New Event: {event_name}!')
            # Compare already localized times
            if event_start_time > current_time:
                await guild.create_scheduled_event(
                    name=event_name,
                    description=event_description,
                    start_time=event_start_time,
                    entity_type=external_event_type,
                    privacy_level=discord.PrivacyLevel.guild_only,
                    location=event_location,
                    end_time=event_end_time,
                    reason="New event created via external Redis call")
                print(f'Successfully created event: {event_name}')
                return True
            print(f'Event in the past!')
            return False
    except discord.HTTPException as e:
        print(f"Discord API Error during event operation: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during event operation: {e}")
        return False

# --- Discord Bot Events ---
def run_discord_bot():
    print(f'{client.user} has connected to Discord!')
    client.run(TOKEN)

# --- Discord Bot Events ---
@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    bot_ready_event.set() # Set the event to signal that the bot is ready
    print("Bot is fully ready to process calls.")

@client.event
async def on_error(event, *args, **kwargs):
    print(f"Error in {event}: {args} {kwargs}")

# --- Main execution ---
if __name__ == "__main__":
    # If running directly, we just start the bot and it blocks
    run_discord_bot()