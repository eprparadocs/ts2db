"""

This is the top of the TS2DB 
"""
from enum import Enum
import logging
import asyncio

from fastapi import FastAPI, HTTPException, Request
import uvicorn


# Valid Logging Le(...,vel flags
class LOGENUM(str, Enum):
    critical = 'critical',
    error = 'error',
    warning = 'warning',
    info = 'info',
    debug = 'debug',
    trace = 'trace'

# Default IP and PORT for the server
DEF_IP = "127.0.0.1"
DEF_PORT = 8000


app = FastAPI()


@app.get("/login",
         description="Log into the service.")
def login():
    """
    Get access to the microservice.
    """
    return {"Hello": "login"}


@app.get("/logout",
         description="Logout of the service.")
def logout():
    """
    Logout of the service.
    """
    return {"Hello": "logout"}


@app.post("/graphQL",
          description="Execute a GraphQL request.")
def graphQL_request(gqlreq):
    """
    Start all GraphQL operations here.
    """
    return {"Hello": "graphQL"}


@app.post("/sql",
          description="Execute a single SQL request")
def sql_request(sqlreq):
    """
    Handle all SQL requests here!
    """
    return{"Hello": "sql"}


async def serverapp(serverip:str, port:int, loglevel:LOGENUM):
    config = uvicorn.Config(app, host=serverip, port=port, log_level=loglevel)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    import sys
    import typer

    def main(serverip:str = typer.Argument(DEF_IP, help="IP address of server", metavar='IPaddress'),
             port:int = typer.Argument(DEF_PORT, help="PORT of control message server", metavar='int'),
             logLevel:LOGENUM =  LOGENUM.info):
            
            # Spin up the server!
            asyncio.run(serverapp(serverip, port, logLevel))

    typer.run(main)
    