//
//  TimedOperation.swift
//  TrueTime
//
//  Created by Michael Sanders on 7/18/16.
//  Copyright © 2016 Instacart. All rights reserved.
//

import Foundation

protocol TimedOperation: class {
    var started: Bool { get }
    var timeout: TimeInterval { get }
    var timer: DispatchSourceTimer? { get set }
    var timerQueue: DispatchQueue { get }

    func debugLog(_ message: @autoclosure () -> String)
    func timeoutError(_ error: NSError)
}

extension TimedOperation {
    func startTimer() {
        cancelTimer()
        timer = DispatchSource.makeTimerSource(flags: [], queue: timerQueue)
        timer?.schedule(deadline: .now() + timeout)
        timer?.setEventHandler {[weak self] in
            guard let strongSelf = self else {
                return
            }
            guard strongSelf.started else { return }
            strongSelf.debugLog("Got timeout for \(self)")
            strongSelf.timeoutError(NSError(trueTimeError: .timedOut))
        }
        timer?.resume()
    }

    func cancelTimer() {
        timer?.cancel()
        timer = nil
    }
}
