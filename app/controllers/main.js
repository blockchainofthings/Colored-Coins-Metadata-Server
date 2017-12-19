var casimir = global.casimir
var handler = casimir.handler
var logger = casimir.logger
var storage = casimir.storage

handler.on('error', function (err) {
  logger.error('>>>>>>', err)
})

var getMetadata = function (req, res, next) {
  storage.getFile(req.data.torrentHash + '.ccm', function (err, data) {
    if (!err) return res.send(JSON.parse(data.Body.toString()))
    logger.error('getMetadata - err = ', err)
    handler.getMetadata(req.data.torrentHash, req.data.sha2, function (err, metadata) {
      if (err) {
        logger.error('getMetadata - err = ', err)
        return next(['Can\'t get metadata, probably there is no file', 400])
      }
      try {
        var file = JSON.stringify(metadata)
        storage.saveFile(file, req.data.torrentHash + '.ccm', function (err, data) {
          if (err) logger.error(err)
          else logger.info('Saved File')
        })
      } catch (e) {
        logger.error('getMetadata - e = ', e)
        return next(['Can\'t get metadata', 400])
      }
      return res.send(metadata)
    })
  })
}

var addMetadata = function (req, res, next) {
  handler.addMetadata(req.data.metadata, function (err, result) {
    if (err) return next(err)
    try {
      var file = JSON.stringify(req.data.metadata)
      storage.saveFile(file, result.torrentHash.toString('hex') + '.ccm', function (err, data) {
        if (err) return next(err)
        return res.send(JSON.stringify({
          torrentHash: result.torrentHash.toString('hex'),
          sha2: result.sha2.toString('hex')
        }))
      })
    } catch (e) {
      logger.error('addMetadata - e = ', e)
      return next(['Can\'t add metadata', 400])
    }
  })
}

var shareMetadata = function (req, res, next) {
  handler.shareMetadata(req.data.torrentHash, function (err) {
    if (err) {
      logger.error('shareMetadata - err = ', err)
      return next(['Can\'t share metadata', 400])
    }
    logger.info('shareMetadata:', req.data.torrentHash)
    return res.status(200).end();
  })
}

var removeMetadata = function (req, res, next) {
  try {
    handler.removeMetadata(req.data.torrentHash, removeResult)
  }
  catch (err) {
    if (err.message.startsWith('No torrent with id ')) {
      // Error because torrent does not exist. It might be that it has already been
      //  (temporarily) removed by the seeding procedure. So just continue processing
      logger.info('removeMetadata - torrent not found to be removed');
      removeResult();
    }
    else {
      removeResult(err);
    }
  }

  function removeResult(err) {
    if (err) {
      logger.error('removeMetadata - err = ', err);
      return next(['Can\'t remove metadata', 400]);
    }
    try {
      storage.deleteFile(req.data.torrentHash + '.ccm', function (err) {
        if (err) logger.error(err);
        else {
          logger.info('Deleted File');
          res.status(200).end();
        }
      })
    } catch (e) {
      logger.error('deleteMetadata - e = ', e);
      return next(['Can\'t delete metadata file from storage', 400]);
    }
  }
};

var isRunning = function (req, res, next) {
  if (casimir.running) {
    res.send('OK')
  } else {
    next('Not running')
  }
}

module.exports = {
  getMetadata: getMetadata,
  addMetadata: addMetadata,
  shareMetadata: shareMetadata,
  removeMetadata: removeMetadata,
  isRunning: isRunning
}
