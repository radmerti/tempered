import json
from abc import ABC, abstractmethod
from os import makedirs, remove
from os.path import exists, expanduser, isfile, join
from functools import partial
from json import load

import aiohttp
import asyncio

from tempered.manager import RequestManager, FatalRequestError, CancelRequestError
from tempered.limits import RandomizedDurationRateLimit, RateLimit


def get_arguments() -> dict:
    from argparse import ArgumentParser

    parser = ArgumentParser(
        description='Process some integers.')

    parser.add_argument(
        "-c", "--config",
        help="API key used for authenticating the requests in the X-Riot-Token header field.",
        type=str,
        default=join('.', 'config.json'))

    parser.add_argument(
        "-s", "--min_season",
        help="Matches older than this season are not downloaded.",
        type=int,
        default=11)

    parser.add_argument(
        "-o", "--output_directory",
        help="Path to a directory in which each match will be saved as a json with"
             "file name {gameID}.json. Defaults to '~/Downloads/lol_matches'.",
        type=str,
        default=join(expanduser('~'), 'Downloads', 'lol_matches'))

    parser.add_argument(
        "-r", "--region",
        help="League of legends region. For exmaple 'euw1'. Defaults to 'euw1'.",
        type=str,
        default='euw1')

    return parser.parse_args()


class RiotRequestManager(RequestManager):
    try_again = (500, 502, 503, 504)
    abort_request = (404, 405, 415)
    raise_error = (400, 401, 403)

    async def _handle_response(self, response: aiohttp.ClientResponse):
        json_body = None

        try:
            json_body = await response.json()
        except Exception:
            pass

        if response.status == 200:
            # decode the body as json
            return json_body

        elif response.status == 429:
            if 'Retry-After' in response.headers:
                timeout = float(response.headers['Retry-After'])
            else:
                timeout = 10.0
            print(f"response 429 limited for {timeout} seconds")
            await asyncio.sleep(timeout)

        elif response.status in self.raise_error:
            raise FatalRequestError(f"request error - status {response.status} - {json_body}")

        elif response.status in self.try_again:
            return None  # try again

        elif response.status in self.abort_request:
            raise CancelRequestError(f"request cancelled - status {response.status} - {json_body}")

        else:
            raise RuntimeError(f"unhandled response: {response.status} - {response}")


async def wakeup_loop(every: float):
    while True:
        await asyncio.sleep(every)


class LolMatchDownloader():
    '''
    0. matches
    1. seed match lists (0-100)
    2. seed summoners
    3. masterleagues
    4. match lists (0-100)
    5. match lists (100+)
    '''

    def __init__(
            self,
            headers_and_limits: ((dict, (RateLimit,)),),
            output_directory: str,
            region: str = 'euw1',
            lol_seasons: (int,)=(10, 11),
            lol_queues: (int,)=(420, 440)):

        self.output_directory = output_directory
        self.lol_seasons = lol_seasons
        self.lol_queues = lol_queues

        if exists(self.output_directory):
            if isfile(self.output_directory):
                raise RuntimeError("output dir exists and is a file")
        else:
            makedirs(self.output_directory)

        self._request_manager = RiotRequestManager(headers_and_limits)

        self._base_url = f"https://{region}.api.riotgames.com"

        self._scheduled_account_ids = {}

    def run(self, seed_summoner_names: (str,)):
        loop = asyncio.get_event_loop()

        loop.create_task(self.schedule_seed_summoners(seed_summoner_names))
        loop.create_task(self.schedule_masterleagues())

        try:
            tasks = (wakeup_loop(1.0),)+self._request_manager.tasks
            loop.run_until_complete(asyncio.gather(*tasks, loop=loop))
        except KeyboardInterrupt:
            print("CTRL-C")

    async def schedule_seed_summoners(self, seed_summoner_names: (str,)):
        for summoner_name in seed_summoner_names:
            print(f"seeding with {summoner_name}")
            summoner_url = (
                f"{self._base_url}/lol/summoner/v4/summoners/by-name/"
                f"{summoner_name}")

            await self._request_manager.schedule(
                summoner_url, self.handle_summoner, priority=2)

    async def schedule_masterleagues(
            self,
            queues: (str,) = ('RANKED_FLEX_SR', 'RANKED_SOLO_5x5')):
        for queue in queues:

            print(f"finding seeds from {queue}")

            masterleagues_url = (
                f"{self._base_url}/lol/league/v4/masterleagues/by-queue/{queue}"
            )

            await self._request_manager.schedule(
                masterleagues_url,
                self.handle_masterleagues,
                priority=3
            )

    async def schedule_matchlist(
            self,
            encrypted_account_id: str,
            begin_index: int = 0,
            priority: int = 4):

        if encrypted_account_id in self._scheduled_account_ids:
            return

        self._scheduled_account_ids[encrypted_account_id] = False

        matchlist_url = (
            f"{self._base_url}/lol/match/v4/matchlists/by-account/"
            f"{encrypted_account_id}?beginIndex={begin_index}"
            f"{''.join('&queue={}'.format(q) for q in self.lol_queues)}"
            f"{''.join('&season={}'.format(s) for s in self.lol_seasons)}"
        )
        await self._request_manager.schedule(
            matchlist_url,
            partial(
                self.handle_matchlist,
                encrypted_account_id=encrypted_account_id),
            priority=priority)

    async def handle_summoner(self, summoner):
        await self.schedule_matchlist(
            summoner['accountId'],
            begin_index=0,
            priority=1)

    async def handle_masterleagues(self, leaguelist: dict):
        await self.schedule_seed_summoners(
            entry['summonerName']
            for entry in leaguelist['entries']
        )

    async def handle_matchlist(self, matchlist: dict, encrypted_account_id: str):
        print(f"got matches {matchlist['startIndex']} to {matchlist['endIndex']} "
              f"for {encrypted_account_id}")

        for match in matchlist['matches']:
            out_path = join(self.output_directory, f"{match['gameId']}.json")
            if exists(out_path):
                with open(out_path, 'r') as in_file:
                    try:
                        match_details = json.load(in_file)
                    except json.JSONDecodeError:
                        remove(out_path)
                        print(f"could not decode {out_path}, file deleted")
                await self.enqueue_match_participants(match_details)
            else:
                match_url = f"{self._base_url}/lol/match/v4/matches/{match['gameId']}"
                await self._request_manager.schedule(match_url, self.handle_match, priority=0)

        if matchlist['endIndex'] == matchlist['totalGames']:
            return

        # await self.schedule_matchlist(
        #     encrypted_account_id,
        #     begin_index=matchlist['endIndex'],
        #     priority=5)

    async def handle_match(self, match_details: dict):
        print(f"save {match_details['gameId']}.json")

        out_path = join(self.output_directory, f"{match_details['gameId']}.json")
        with open(out_path, 'w') as out_file:
            json.dump(match_details, out_file)

        await self.enqueue_match_participants(match_details)

    async def enqueue_match_participants(self, match_details: dict):
        for participant_identity in match_details['participantIdentities']:
            encrypted_account_id = participant_identity['player']['currentAccountId']

            await self.schedule_matchlist(
                encrypted_account_id,
                begin_index=0,
                priority=4)

def main():
    args = get_arguments()

    with open(args.config, 'r') as key_file:
        config = load(key_file)

    downloader = LolMatchDownloader(
        tuple(
            (
                {"X-Riot-Token": key},
                tuple(
                    RandomizedDurationRateLimit(seconds, count)
                    for seconds, count in limits
                )
            )
            for key, limits in config['keys_and_limits']
        ),
        args.output_directory
    )

    downloader.run(config['seed_summoners'])


if __name__ == "__main__":
    main()
