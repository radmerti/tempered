import json
from abc import ABC, abstractmethod
from os import makedirs, remove
from os.path import exists, expanduser, isfile, join
from functools import partial

import aiohttp
import asyncio

from tempered.manager import RequestManager
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
            raise RuntimeError("access key expired")
        else:
            raise RuntimeError(f"unhandled response: {response.status} - {response}")


class LolMatchDownloader():
    def __init__(
            self,
            headers_and_limits: (dict, (RateLimit,)),
            output_directory: str,
            region: str = 'euw1'):

        self.output_directory = output_directory

        if exists(self.output_directory):
            if isfile(self.output_directory):
                raise RuntimeError("output dir exists and is a file")
        else:
            makedirs(self.output_directory)

        self._request_manager = RiotRequestManager(headers_and_limits)

        self._base_url = f"https://{region}.api.riotgames.com"

    def run(self, seed_summoner_names: (str,)):
        loop = asyncio.get_event_loop()

        loop.create_task(self.schedule_seed_summoners(seed_summoner_names))

        try:
            loop.run_forever()
        except Exception as exception:
            print(f"error running loop: {exception}")
            raise

    async def schedule_seed_summoners(self, seed_summoner_names: (str,)):
        for summoner_name in seed_summoner_names:
            summoner_url = (
                f"{self._base_url}/lol/summoner/v4/summoners/by-name/"
                f"{summoner_name}")

            await self._request_manager.schedule(
                summoner_url, self.handle_summoner, priority=0)


    async def handle_summoner(self, summoner):
        matchlist_url = (
            f"{self._base_url}/lol/match/v4/matchlists/by-account/"
            f"{summoner['accountId']}?beginIndex=0"
        )
        await self._request_manager.schedule(
            matchlist_url,
            partial(
                self.handle_matchlist,
                encrypted_account_id=summoner['accountId']),
            priority=2)

    async def handle_matchlist(self, matchlist: dict, encrypted_account_id: str):
        for match in matchlist['matches']:
            match_details = None

            out_path = join(self.output_directory, f"{match['gameId']}.json")
            if exists(out_path):
                with open(out_path, 'r') as in_file:
                    try:
                        match_details = json.load(in_file)
                    except json.JSONDecodeError:
                        remove(out_path)
                        print(f"could not decode {out_path}, file deleted")

            if match_details is None:
                match_url = f"{self._base_url}/lol/match/v4/matches/{match['gameId']}"

                await self._request_manager.schedule(match_url, self.handle_match, priority=1)

        if matchlist['endIndex'] == matchlist['totalGames']:
            return

        matchlist_url = (
            f"{self._base_url}/lol/match/v4/matchlists/by-account/"
            f"{encrypted_account_id}?beginIndex={matchlist['endIndex']}"
        )
        await self._request_manager.schedule(
            matchlist_url,
            partial(
                self.handle_matchlist,
                encrypted_account_id=encrypted_account_id),
            priority=3)

    async def handle_match(self, match_details: dict):
        print(f"save {match_details['gameId']}.json")

        out_path = join(self.output_directory, f"{match_details['gameId']}.json")
        with open(out_path, 'w') as out_file:
            json.dump(match_details, out_file)

        for participant_identity in match_details['participantIdentities']:
            encrypted_account_id = participant_identity['player']['currentAccountId']
            matchlist_url = (
                f"{self._base_url}/lol/match/v4/matchlists/by-account/"
                f"{encrypted_account_id}?beginIndex=0"
            )
            await self._request_manager.schedule(
                matchlist_url,
                partial(
                    self.handle_matchlist,
                    encrypted_account_id=encrypted_account_id),
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
