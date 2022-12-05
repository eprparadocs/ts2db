"""

This is the top of the TS2DB 
"""
from enum import Enum
import logging
import asyncio
import json


from fastapi import FastAPI, HTTPException, Request
import uvicorn

from ts2db.database import createdb

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


@app.post("/create",
          description="Create some resource, database etc..")
def create_request(creq):
    """
    Input into this function is a JSON document that detauls exactly
    what is being created.
    
    {
      <operation>: {
      }
    }
    
    <operation> is "table", for create a database table.
    """
    
    opstable = {"table":createdb}
    if 'operation' not in creq:
        raise HTTPException(400, "Operation to perform is missing!")
    op = creq["operation"]
    if op not in opstable:
        raise HTTPException(400, "Invalid operations '%s' specified!" % (op))
    rc = opstable[op](creq)
    return rc


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
    