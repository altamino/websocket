from typing import Union


class User:
    @staticmethod
    def OwnNonSensetiveProfile(
        row, ndcId: int = 0, extensions: dict = {}, membershipStatus: int = 0
    ):
        """
        ndcId for another communities
        triggerUserId is who triggered this shit
        """
        ndcId = int(ndcId)
        return {
            "status": row["status"],
            "uid": row["id"],
            "modifiedTime": row["modifiedTime"],
            "createdTime": row["createdTime"],
            "role": row.get("role", 0),
            "aminoId": row.get("aminoId"),
            "nickname": row["nickname"],
            "tagList": row.get("tagList", []),
            "mediaList": [],
            "icon": row.get("icon"),
            "accountMembershipStatus": int(row.get("isPaidSubscriber", 0)),
            "ndcId": ndcId,  # 0 is global
            "isGlobal": ndcId == 0,
            "reputation": 0,  # if ndcId == 0 else row["reputation"],
            "level": 0,  # if ndcId == 0 else row["level"],
            "mood": None,  # if ndcId == 0 else row["mood"],
            "content": ((row.get("description") or "").strip()),
            "joinedCount": len(row["following"]),
            "followingStatus": 0,
            "membersCount": len(row["whoFollows"]),
            "storiesCount": 0,
            "blogsCount": (
                0 if ndcId else 0
            ),  # [TODO] when communitues will be implemented do that
            "postsCount": (
                0 if ndcId else 0
            ),  # [TODO] when communitues will be implemented do that
            "backgroundColor": row.get("backgroundColor"),
            "extensions": {
                "tagList": row.get("tagList", []),
                "customTitles": row.get("titles", []),
                "backgroundColor": row.get("backgroundColor"),
                "style": {
                    "backgroundColor": row.get("backgroundColor"),
                    "backgroundMediaList": [],
                },
                "isMemberOfTeamAmino": row.get("isTeamMember", False),
            }
            | extensions,
            "moodSticker": (
                # None if ndcId == 0 else row["mood"]
                None
            ),  # [TODO]: check wtf is this
            "consecutiveCheckInDays": (
                # None if ndcId == 0 else row["consecutiveDaysOfCheckIns"]
                None
            ),  # [TODO] when communitues will be implemented do that
            "onlineStatus": 2,  # [TODO]: check wtf is this
            "isNicknameVerified": bool(row.get("isVerified", False)),
            "verified": bool(len(row.get("tagList", []))),  # this fixes tagList! :D
            "notificationSubscriptionStatus": 0,
            "pushEnabled": True,
            "membershipStatus": membershipStatus,
            "commentsCount": len(row.get("wall", [])),
        }
