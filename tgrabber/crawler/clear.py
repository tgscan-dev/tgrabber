from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import func

from tgrabber.dao.database import DBSession
from tgrabber.dao.models import Room


def remove_bad_link() :
    with DBSession() as db_sess :
        web_crawl_batch = 1
        while True :
            rooms = db_sess.query(Room).filter(
                Room.web_crawl_batch < web_crawl_batch,
            ).limit(100).offset(0).all()
            if len(rooms) == 0 :
                logger.info(f"no more rooms to update")
                break
            for room in rooms :
                link = room.link
                room.web_crawl_batch = web_crawl_batch
                # url parse
                url = urlparse(link)
                path = url.path
                if path.startswith("/joinchat/") :
                    db_sess.delete(room)
                    logger.info(f"delete room {room.link}")
                    continue
                path_split = path.split("/")
                if len(path_split) > 2 :
                    path = path_split[1]
                    room.link = f"{url.scheme}://{url.netloc}/{path}".lower()

                    if db_sess.query(Room).filter(Room.link == room.link).count() > 1 :
                        db_sess.delete(room)
                        logger.info(f"duplicate room {room.link}, delete")
                        continue
                    logger.info(f"update room from {link} to {room.link}")
            db_sess.commit()


def remove_duplicated() :
    global link
    with DBSession() as db_sess :
        while True :
            links = db_sess.query(Room.link).group_by(Room.link).having(func.count(Room.link) > 1).limit(100).all()
            if len(links) == 0 :
                logger.info(f"no more rooms to update")
                break
            for link in links :
                rooms = db_sess.query(Room).filter(Room.link == link[0]).all()
                for x in rooms[1 :] :
                    db_sess.delete(x)
                    logger.info(f"delete room {x.link}")
            db_sess.commit()


if __name__ == '__main__':
    # remove_bad_link()
    remove_duplicated()