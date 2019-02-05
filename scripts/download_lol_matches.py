import json
from abc import ABC, abstractmethod
from os import makedirs, remove
from os.path import exists, expanduser, isfile, join
from functools import partial

import aiohttp
import asyncio

from tempered.manager import RequestManager, KeyExpiredError, InternalServerError
from tempered.limits import RandomizedDurationRateLimit, RateLimit


def get_arguments() -> dict:
    from argparse import ArgumentParser

    parser = ArgumentParser(
        description='Process some integers.')

    parser.add_argument(
        "seed_summoner_name",
        help="Summoner name of some user that is used to seed the search for matches.",
        type=str)

    parser.add_argument(
        "api_key",
        help="API key used for authenticating the requests in the X-Riot-Token header field.",
        type=str)

    parser.add_argument(
        "--min_season",
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
        "--region",
        help="League of legends region. For exmaple 'euw1'. Defaults to 'euw1'.",
        type=str,
        default='euw1')

    return parser.parse_args()


class RiotRequestManager(RequestManager):
    @staticmethod
    async def _handle_response(response: aiohttp.ClientResponse):
        if response.status == 200:
            # decode the body as json
            return await response.json()
        elif response.status == 429:
            if 'Retry-After' in response.headers:
                timeout = float(response.headers['Retry-After'])
            else:
                timeout = 10.0
            print(f"response 429 limited for {timeout} seconds")
            await asyncio.sleep(timeout)
        elif response.status == 403:
            raise KeyExpiredError("status 403 - access key expired")
        elif response.status == 500:
            return None  # try again
        else:
            raise RuntimeError(f"unhandled response: {response.status} - {response}")


async def wakeup_loop(every: float):
    while True:
        await asyncio.sleep(every)


class LolMatchDownloader():
    def __init__(
            self,
            headers_and_limits: (dict, (RateLimit,)),
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

        try:
            tasks = (wakeup_loop(1.0),)+self._request_manager.tasks
            loop.run_until_complete(asyncio.gather(*tasks, loop=loop))
        except KeyboardInterrupt:
            print("CTRL-C")

    async def schedule_seed_summoners(self, seed_summoner_names: (str,)):
        for summoner_name in seed_summoner_names:
            summoner_url = (
                f"{self._base_url}/lol/summoner/v4/summoners/by-name/"
                f"{summoner_name}")

            await self._request_manager.schedule(
                summoner_url, self.handle_summoner, priority=0)

    async def schedule_matchlist(
            self,
            encrypted_account_id: str,
            begin_index: int = 0,
            priority: int = 0):

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
            priority=2)

    async def handle_matchlist(self, matchlist: dict, encrypted_account_id: str):
        print(f"got matches {matchlist['startIndex']} to {matchlist['endIndex']} for {encrypted_account_id}")

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
                await self._request_manager.schedule(match_url, self.handle_match, priority=1)

        if matchlist['endIndex'] == matchlist['totalGames']:
            return

        # await self.schedule_matchlist(
        #     encrypted_account_id,
        #     begin_index=matchlist['endIndex'],
        #     priority=3)

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
                priority=2)

def main():
    args = get_arguments()

    downloader = LolMatchDownloader(
        (
            (
                {"X-Riot-Token": args.api_key},
                (RandomizedDurationRateLimit(1.0, 20),
                 RandomizedDurationRateLimit(120.0, 100))
            ),
        ),
        args.output_directory
    )

    downloader.run((args.seed_summoner_name,))


if __name__ == "__main__":
    main()
