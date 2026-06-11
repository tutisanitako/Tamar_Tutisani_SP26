PASSING_GRADE = 8


class Trainee:
    def __init__(self, name, surname):
        self.name = name
        self.surname = surname
        self.mark = 0
        self.visited_lectures = 0
        self.missed_lectures = 0
        self.done_home_tasks = 0
        self.missed_home_tasks = 0

    def visit_lecture(self):
        # Track the lecture and reward 1 point.
        self.visited_lectures += 1
        self._add_points(1)

    def do_homework(self):
        # Track the homework and reward 2 points.
        self.done_home_tasks += 2
        self._add_points(2)

    def miss_lecture(self):
        # Track the missed lecture and deduct 1 point.
        self.missed_lectures -= 1
        self._subtract_points(1)

    def miss_homework(self):
        # Track the missed homework and deduct 2 points.
        self.missed_home_tasks -= 2
        self._subtract_points(2)

    def _add_points(self, points: int):
        # Cap the mark at 10 — going over isn't possible.
        self.mark = min(self.mark + points, 10)

    def _subtract_points(self, points):
        # Floor the mark at 0 — it can't go negative.
        self.mark = max(self.mark - points, 0)

    def is_passed(self):
        if self.mark >= PASSING_GRADE:
            print("Good job!")
        else:
            print(f"You need to get {PASSING_GRADE - self.mark} more points. Try to do your best!")

    def __str__(self):
        status = (
            f"Trainee {self.name.title()} {self.surname.title()}:\n"
            f"done homework {self.done_home_tasks} points;\n"
            f"missed homework {self.missed_home_tasks} points;\n"
            f"visited lectures {self.visited_lectures} points;\n"
            f"missed lectures {self.missed_lectures} points;\n"
            f"current mark {self.mark};\n"
        )
        return status